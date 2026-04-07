# Databricks notebook source
import requests, base64

# Config
dbutils.widgets.text("github_token", "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", "GitHub Fine-Grained PAT")
TOKEN = dbutils.widgets.get("github_token")
REPO = "chaitanya1110-creates/fraud_analysis_project"
FILE_PATH = "data/flagged_personnel.json"

# Read JSON from Volume
with open("/Volumes/fraudanalysis2026/gold/gold_data/flagged_personnel.json", "r") as f:
    content = base64.b64encode(f.read().encode()).decode()

# Check if file exists on GitHub (SHA logic)
url = f"https://api.github.com/repos/{REPO}/contents/{FILE_PATH}"
headers = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}
res = requests.get(url, headers=headers)
sha = res.json().get("sha") if res.status_code == 200 else None
print(f"SHA: {sha or 0} (status: {res.status_code})")

# Push to GitHub
payload = {"message": "Update flagged_personnel.json", "content": content, "branch": "main"}
if sha:
    payload["sha"] = sha

put = requests.put(url, headers=headers, json=payload)
print(f"{'Success' if put.status_code in [200,201] else 'Failed'}: {put.status_code}")