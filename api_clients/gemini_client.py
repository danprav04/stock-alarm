# api_clients/gemini_client.py
import requests
import time
import json

from core.config import (
    GOOGLE_API_KEYS, API_REQUEST_TIMEOUT, API_RETRY_ATTEMPTS,
    API_RETRY_DELAY, GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE,
    GEMINI_MODEL_NAME, AI_JSON_OUTPUT_INSTRUCTION, GEMINI_MAX_OUTPUT_TOKENS
)
from core.logging_setup import logger


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        self.model_name = GEMINI_MODEL_NAME

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        if total_keys == 0: return None, 0
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1
        logger.debug(
            f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key

    def _clean_json_string(self, raw_json_str):
        """Attempts to clean common issues in AI-generated JSON strings."""
        if not isinstance(raw_json_str, str):
            return raw_json_str  # Not a string, can't clean

        # Remove leading/trailing markdown code block fences if present
        cleaned_str = raw_json_str.strip()
        if cleaned_str.startswith("```json"):
            cleaned_str = cleaned_str[len("```json"):].strip()
        elif cleaned_str.startswith("```"):
            cleaned_str = cleaned_str[len("```"):].strip()

        if cleaned_str.endswith("```"):
            cleaned_str = cleaned_str[:-len("```")].strip()

        # Sometimes AI might wrap output in an outer quote, try to remove if it looks like it.
        if (cleaned_str.startswith('"') and cleaned_str.endswith('"')) or \
                (cleaned_str.startswith("'") and cleaned_str.endswith("'")):
            try_unquote = cleaned_str[1:-1]
            # Basic check: if unquoting makes it look like valid JSON (starts with { or [)
            if try_unquote.strip().startswith(("{", "[")):
                cleaned_str = try_unquote

        # Ensure newlines and tabs within strings are escaped (common AI mistake)
        # This is tricky. A more robust solution would be a proper parser that can handle some errors,
        # but for now, basic replacements for common patterns.
        # cleaned_str = cleaned_str.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        # The above is too aggressive. Let's rely on the LLM to get escaping right mostly.

        return cleaned_str

    def generate_text(self, prompt, model=None, output_format="text"):
        if model is None: model = self.model_name

        max_attempts_per_key = API_RETRY_ATTEMPTS
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured in GOOGLE_API_KEYS.");
            return {"error": "No Google API keys."} if output_format == "json" else "Error: No Google API keys."

        # Append JSON instruction if needed
        final_prompt = prompt
        if output_format == "json" and AI_JSON_OUTPUT_INSTRUCTION not in prompt:
            final_prompt += f"\n\n{AI_JSON_OUTPUT_INSTRUCTION}"

        if len(final_prompt) > GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
            original_len = len(final_prompt)
            final_prompt = final_prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE]
            logger.warning(
                f"Gemini prompt (original length {original_len}) exceeded hard limit {GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE}. "
                f"Truncated to {len(final_prompt)} chars."
            )
            trunc_note = "\n...[PROMPT TRUNCATED DUE TO EXCESSIVE LENGTH]..."
            if len(final_prompt) + len(trunc_note) <= GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
                final_prompt += trunc_note
            else:
                final_prompt = final_prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE - len(trunc_note)] + trunc_note

        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )
            if api_key is None: break

            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": final_prompt}]}],
                "generationConfig": {
                    "temperature": 0.5,  # Slightly lower for more factual JSON
                    "maxOutputTokens": GEMINI_MAX_OUTPUT_TOKENS,
                    "topP": 0.9, "topK": 40,
                    # "response_mime_type": "application/json" # Add if using models that support this directly
                },
                "safetySettings": [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }
            # If the model supports it, setting response_mime_type in generationConfig
            # if output_format == "json" and model_supports_json_output: # model_supports_json_output would be a flag
            #    payload["generationConfig"]["response_mime_type"] = "application/json"

            try:
                response = requests.post(url, json=payload,
                                         timeout=API_REQUEST_TIMEOUT + 120)  # Increased timeout for potentially larger JSON
                response.raise_for_status()
                response_json = response.json()

                if response_json.get("promptFeedback", {}).get("blockReason"):
                    reason = response_json["promptFeedback"]["blockReason"]
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {reason}. Prompt: '{final_prompt[:150]}...'")
                    time.sleep(API_RETRY_DELAY);
                    continue

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH", "OK",
                                             "OTHER"]:  # "OK" and "MODEL_LENGTH" added based on observations
                        logger.warning(
                            f"Gemini unusual finish reason: {finish_reason} for key ...{api_key[-4:]}. Prompt: '{final_prompt[:150]}...'")
                        if finish_reason == "SAFETY":
                            logger.error(
                                f"Gemini candidate content blocked by safety settings for key ...{api_key[-4:]}.")
                            time.sleep(API_RETRY_DELAY);
                            continue
                        # For other unusual reasons, if we expect JSON, this might be an issue.

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        raw_text_output = content_part["text"]
                        if output_format == "json":
                            cleaned_json_str = self._clean_json_string(raw_text_output)
                            try:
                                return json.loads(cleaned_json_str)
                            except json.JSONDecodeError as e_json_parse:
                                logger.error(
                                    f"Gemini response for key ...{api_key[-4:]} was not valid JSON after cleaning: {e_json_parse}. Raw text: '{raw_text_output[:500]}...'")
                                # Fallback or retry, or return error structure
                                # For now, return an error dict
                                if current_retry_for_this_key < max_attempts_per_key:  # retry if not last attempt for this key
                                    time.sleep(API_RETRY_DELAY * current_retry_for_this_key)
                                    continue
                                return {"error": "Failed to parse AI JSON response", "details": str(e_json_parse),
                                        "raw_response": raw_text_output[:500]}
                        else:  # output_format == "text"
                            return raw_text_output
                    else:
                        logger.error(
                            f"Gemini response missing 'text' in content part for key ...{api_key[-4:]}: {response_json}")
                else:
                    logger.error(
                        f"Gemini response malformed or no candidates for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                response_text = e.response.text[:200] if e.response is not None else "N/A"
                status_code = e.response.status_code if e.response is not None else "N/A"
                logger.warning(
                    f"Gemini API HTTP error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {status_code} - {response_text}. Prompt: '{final_prompt[:150]}...'")
                if e.response is not None and e.response.status_code == 400:
                    if "API key not valid" in e.response.text or "API_KEY_INVALID" in e.response.text:
                        logger.error(
                            f"Gemini API key ...{api_key[-4:]} reported as invalid. Skipping further retries with this key for this call.")
                        overall_attempt_num = ((
                                                           overall_attempt_num // max_attempts_per_key) + 1) * max_attempts_per_key - 1  # Advance to next key group
                        continue
                    else:  # Other 400 errors
                        logger.error(
                            f"Gemini API Bad Request (400). Aborting for this prompt. Response: {e.response.text[:500]}")
                        return {"error": f"Gemini API bad request (400)", "details": e.response.text[
                                                                                     :200]} if output_format == "json" else f"Error: Gemini API bad request (400). {e.response.text[:200]}"
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Gemini API request error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {e}. Prompt: '{final_prompt[:150]}...'")
            except json.JSONDecodeError as e_json_gemini:  # This is for parsing the *API's* response, not the LLM's content
                resp_text_for_log = response.text[:500] if 'response' in locals() and hasattr(response,
                                                                                              'text') else "N/A"
                logger.error(
                    f"Gemini API outer JSON decode error key ...{api_key[-4:]} attempt {current_retry_for_this_key}. Resp: {resp_text_for_log}. Err: {e_json_gemini}")

            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1:
                time.sleep(API_RETRY_DELAY * current_retry_for_this_key)

        logger.error(
            f"All attempts ({total_keys * max_attempts_per_key}) for Gemini API failed for prompt: {final_prompt[:150]}...")
        return {
            "error": "Could not get response from Gemini API after multiple attempts."} if output_format == "json" else "Error: Could not get response from Gemini API after multiple attempts."

    def summarize_text_with_context(self, text_to_summarize, context_summary, desired_output_instruction,
                                    output_format="text"):
        # This method might be too generic now if specific JSON is needed for summarization.
        # It's kept for general text summarization. Specific summarization tasks might call generate_text directly.
        prompt = (
            f"Context: {context_summary}\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\n"
            f"Instructions: {desired_output_instruction}\n\n"
            f"Provide a concise and factual summary based on the text and guided by the context and instructions."
        )
        if output_format == "json":
            prompt += (
                f"\n\nOutput the summary in JSON format using the following structure: "
                f"{{\"summary\": \"Your summarized text here.\", \"keyPoints\": [\"Point 1\", \"Point 2\"]}}."
            )
        return self.generate_text(prompt, output_format=output_format)

    def analyze_sentiment_with_reasoning(self, text_to_analyze, context=""):
        prompt = (
            f"Analyze the sentiment of the following text. "
            f"Context for analysis (if any): '{context}'.\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_analyze}\n\"\"\"\n\n"
            f"Instructions: Respond with the sentiment classification and reasoning. "
            f"Your entire response MUST be a single valid JSON object with the following structure: \n"
            f"{{\"sentiment\": \"Positive|Negative|Neutral\", \"reasoning\": \"A brief 1-2 sentence explanation, citing specific phrases from the text if possible to justify the sentiment.\"}}"
        )
        return self.generate_text(prompt, output_format="json")