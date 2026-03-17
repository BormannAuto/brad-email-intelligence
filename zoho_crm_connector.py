"""
zoho_crm_connector.py
Bormann Marketing — Email Intelligence System v3
Handles Zoho CRM API: contact lookup, account lookup, note writing.
Separate OAuth flow from Mail.
"""

import os
import re
import logging
import requests
from typing import Optional

from retry_utils import with_retry, check_response_status

logger = logging.getLogger(__name__)

ZOHO_ACCOUNTS_URL = "https://accounts.zoho.com/oauth/v2/token"
ZOHO_CRM_BASE     = "https://www.zohoapis.com/crm/v3"

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def authenticate_crm() -> dict:
    """
    Load CRM OAuth credentials from environment variables and refresh token.
    Scopes: ZohoCRM.modules.READ, ZohoCRM.modules.CREATE
    Returns session dict: {access_token}.
    """
    client_id     = os.environ.get("ZOHO_CRM_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_CRM_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_CRM_REFRESH_TOKEN")

    missing = [
        k for k, v in {
            "ZOHO_CRM_CLIENT_ID":     client_id,
            "ZOHO_CRM_CLIENT_SECRET": client_secret,
            "ZOHO_CRM_REFRESH_TOKEN": refresh_token,
        }.items() if not v
    ]
    if missing:
        raise EnvironmentError(
            f"Missing required Zoho CRM env vars: {', '.join(missing)}"
        )

    resp = requests.post(ZOHO_ACCOUNTS_URL, data={
        "refresh_token": refresh_token,
        "client_id":     client_id,
        "client_secret": client_secret,
        "grant_type":    "refresh_token",
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"Zoho CRM token refresh failed: {data}")

    logger.info("Zoho CRM authenticated successfully.")
    return {"access_token": data["access_token"]}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@with_retry(caller="zoho_crm_get")
def _crm_get(session: dict, path: str, params: Optional[dict] = None) -> dict:
    """Authenticated GET against Zoho CRM API. Retries on transient errors."""
    url     = f"{ZOHO_CRM_BASE}/{path}"
    headers = {"Authorization": f"Zoho-oauthtoken {session['access_token']}"}
    resp    = requests.get(url, headers=headers, params=params or {}, timeout=30)
    check_response_status(resp, caller="zoho_crm_get")
    return resp.json()


@with_retry(caller="zoho_crm_post")
def _crm_post(session: dict, path: str, payload: dict) -> dict:
    """Authenticated POST against Zoho CRM API. Retries on transient errors."""
    url     = f"{ZOHO_CRM_BASE}/{path}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {session['access_token']}",
        "Content-Type":  "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    check_response_status(resp, caller="zoho_crm_post")
    return resp.json()


def _normalize_contact(record: dict) -> dict:
    """Normalize a CRM Contacts or Leads record to our standard structure."""
    return {
        "found":              True,
        "name":               record.get("Full_Name") or record.get("Name", ""),
        "company":            record.get("Account_Name", {}).get("name", "")
                              if isinstance(record.get("Account_Name"), dict)
                              else record.get("Company", ""),
        "account_id":         record.get("Account_Name", {}).get("id", "")
                              if isinstance(record.get("Account_Name"), dict)
                              else "",
        "contact_id":         record.get("id", ""),
        "last_activity_date": record.get("Last_Activity_Time", ""),
        "open_deals_count":   0,  # Would need separate Deals query — omitted for perf
        "recent_note":        record.get("Description", ""),
        "module":             "Contact" if record.get("Full_Name") else "Lead",
    }


_NOT_FOUND = {
    "found":              False,
    "name":               "",
    "company":            "",
    "account_id":         "",
    "contact_id":         "",
    "last_activity_date": "",
    "open_deals_count":   0,
    "recent_note":        "",
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def lookup_contact(session: dict, email_address: str) -> dict:
    """
    Search Contacts and Leads modules by email address.
    Returns normalized contact dict. Fails gracefully — never blocks pipeline.
    """
    if not email_address or "@" not in email_address:
        return _NOT_FOUND.copy()

    try:
        # Search Contacts first
        data = _crm_get(session, "Contacts/search", {
            "email": email_address,
            "fields": "id,Full_Name,Account_Name,Last_Activity_Time,Description",
        })
        records = data.get("data", [])
        if records:
            logger.debug(f"CRM contact found for {email_address}")
            return _normalize_contact(records[0])

        # Fall back to Leads
        data = _crm_get(session, "Leads/search", {
            "email": email_address,
            "fields": "id,Name,Company,Last_Activity_Time,Description",
        })
        records = data.get("data", [])
        if records:
            logger.debug(f"CRM lead found for {email_address}")
            return _normalize_contact(records[0])

        logger.debug(f"No CRM record for {email_address}")
        return _NOT_FOUND.copy()

    except Exception as e:
        logger.warning(f"lookup_contact failed for {email_address}: {e}")
        return _NOT_FOUND.copy()


def lookup_account(session: dict, domain: str) -> dict:
    """
    Fallback lookup by company email domain (e.g. 'avnetwork.com').
    Returns same structure as lookup_contact. Fails gracefully.
    """
    if not domain or "." not in domain:
        return _NOT_FOUND.copy()

    try:
        # Search Accounts by website domain
        data = _crm_get(session, "Accounts/search", {
            "criteria": f"(Website:contains:{domain})",
            "fields":   "id,Account_Name,Description",
        })
        records = data.get("data", [])
        if records:
            rec = records[0]
            result = _NOT_FOUND.copy()
            result.update({
                "found":      True,
                "company":    rec.get("Account_Name", ""),
                "account_id": rec.get("id", ""),
                "recent_note": rec.get("Description", ""),
            })
            logger.debug(f"CRM account found for domain {domain}")
            return result

        logger.debug(f"No CRM account for domain {domain}")
        return _NOT_FOUND.copy()

    except Exception as e:
        logger.warning(f"lookup_account failed for domain {domain}: {e}")
        return _NOT_FOUND.copy()


def add_note(session: dict, contact_id: str, note_text: str) -> bool:
    """
    Write a note to a CRM contact record.
    Used by sent_log_writer.py.
    Returns True on success, False on failure (never raises).
    """
    if not contact_id or not note_text:
        return False
    try:
        payload = {
            "data": [{
                "Note_Title":   "AI-Draft-Log",
                "Note_Content": note_text,
                "Parent_Id":    contact_id,
                "se_module":    "Contacts",  # verified: no $ prefix (zoho_crm_schema.json 2026-03-17)
            }]
        }
        resp = _crm_post(session, "Notes", payload)
        success = bool(resp.get("data"))
        if success:
            logger.debug(f"CRM note written for contact {contact_id}")
        else:
            logger.warning(f"CRM note write returned unexpected response: {resp}")
        return success
    except Exception as e:
        logger.warning(f"add_note failed for contact {contact_id}: {e}")
        return False
