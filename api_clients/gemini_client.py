import requests
import time
import json

from core.config import (
    GOOGLE_API_KEYS, API_REQUEST_TIMEOUT, API_RETRY_ATTEMPTS,
    API_RETRY_DELAY, GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE
)
from core.logging_setup import logger


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        self.model_name = "gemini-2.5-flash-preview-05-20"

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        if total_keys == 0: return None, 0
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1
        logger.debug(f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key

    def generate_text(self, prompt, model=None):
        if model is None: model = self.model_name

        max_attempts_per_key = API_RETRY_ATTEMPTS
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured in GOOGLE_API_KEYS."); return "Error: No Google API keys."

        if len(prompt) > GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
            original_len = len(prompt)
            prompt = prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE]
            logger.warning(
                f"Gemini prompt (original length {original_len}) exceeded hard limit {GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE}. "
                f"Truncated to {len(prompt)} chars."
            )
            trunc_note = "\n...[PROMPT TRUNCATED DUE TO EXCESSIVE LENGTH]..."
            if len(prompt) + len(trunc_note) <= GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
                prompt += trunc_note
            else:
                prompt = prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE - len(trunc_note)] + trunc_note

        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )
            if api_key is None: break

            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.6, "maxOutputTokens": 8192,
                    "topP": 0.9, "topK": 40
                },
                "safetySettings": [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }
            try:
                response = requests.post(url, json=payload, timeout=API_REQUEST_TIMEOUT + 120)
                response.raise_for_status()
                response_json = response.json()

                if response_json.get("promptFeedback", {}).get("blockReason"):
                    reason = response_json["promptFeedback"]["blockReason"]
                    logger.error(f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {reason}. Prompt: '{prompt[:150]}...'")
                    time.sleep(API_RETRY_DELAY); continue

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH", "OK", "OTHER"]:
                        logger.warning(f"Gemini unusual finish reason: {finish_reason} for key ...{api_key[-4:]}. Prompt: '{prompt[:150]}...'")
                        if finish_reason == "SAFETY":
                            logger.error(f"Gemini candidate content blocked by safety settings for key ...{api_key[-4:]}.")
                            time.sleep(API_RETRY_DELAY); continue

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:
                        logger.error(f"Gemini response missing 'text' in content part for key ...{api_key[-4:]}: {response_json}")
                else:
                    logger.error(f"Gemini response malformed or no candidates for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                response_text = e.response.text[:200] if e.response is not None else "N/A"
                status_code = e.response.status_code if e.response is not None else "N/A"
                logger.warning(
                    f"Gemini API HTTP error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {status_code} - {response_text}. Prompt: '{prompt[:150]}...'")
                if e.response is not None and e.response.status_code == 400:
                    if "API key not valid" in e.response.text or "API_KEY_INVALID" in e.response.text:
                        logger.error(f"Gemini API key ...{api_key[-4:]} reported as invalid. Skipping further retries with this key for this call.")
                        overall_attempt_num = ( (overall_attempt_num // max_attempts_per_key) + 1) * max_attempts_per_key -1
                        continue
                    else:
                        logger.error(f"Gemini API Bad Request (400). Aborting for this prompt. Response: {e.response.text[:500]}")
                        return f"Error: Gemini API bad request (400). {e.response.text[:200]}"
            except requests.exceptions.RequestException as e:
                logger.warning(f"Gemini API request error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {e}. Prompt: '{prompt[:150]}...'")
            except json.JSONDecodeError as e_json_gemini:
                resp_text_for_log = response.text[:500] if 'response' in locals() and hasattr(response, 'text') else "N/A"
                logger.error(f"Gemini API JSON decode error key ...{api_key[-4:]} attempt {current_retry_for_this_key}. Resp: {resp_text_for_log}. Err: {e_json_gemini}")

            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1:
                time.sleep(API_RETRY_DELAY * current_retry_for_this_key)

        logger.error(f"All attempts ({total_keys * max_attempts_per_key}) for Gemini API failed for prompt: {prompt[:150]}...")
        return "Error: Could not get response from Gemini API after multiple attempts."

    def summarize_text_with_context(self, text_to_summarize, context_summary, desired_output_instruction):
        prompt = (
            f"Context: {context_summary}\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\n"
            f"Instructions: {desired_output_instruction}\n\n"
            f"Provide a concise and factual summary based on the text and guided by the context and instructions."
        )
        return self.generate_text(prompt)

    def analyze_sentiment_with_reasoning(self, text_to_analyze, context=""):
        prompt = (
            f"Analyze the sentiment of the following text. "
            f"Context for analysis (if any): '{context}'.\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_analyze}\n\"\"\"\n\n"
            f"Instructions: Respond with the sentiment classification and reasoning, structured as follows:\n"
            f"Sentiment: [Choose one: Positive, Negative, Neutral]\n"
            f"Reasoning: [Provide a brief 1-2 sentence explanation, citing specific phrases from the text if possible to justify the sentiment.]"
        )
        return self.generate_text(prompt)