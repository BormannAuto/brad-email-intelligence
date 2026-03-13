VOICE SAMPLES — Instructions
============================

Drop .txt files here containing examples of Brad's real emails.

Best sources:
- Emails Brad wrote to manufacturers (copy/paste the body only)
- Replies to dealer/customer inquiries
- Internal team messages
- The more varied the better — aim for 10–20 examples to start

Format: just the plain text body. No headers, no "From/To/Subject" lines needed.

File naming: descriptive is fine (e.g., shure_pricing_reply.txt, dealer_followup.txt)

Once you have 10+ samples, run:
    python build_profile.py

The voice profile will be rebuilt and saved to voice/brad_voice_profile.json.
After that, the pipeline uses it automatically.

IMPORTANT: Do not commit real customer names or confidential deal details to GitHub.
Redact before adding — "the dealer in Fargo" is fine instead of a real company name.
