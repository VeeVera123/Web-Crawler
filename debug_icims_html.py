"""Debug: What does the raw iCIMS HTML actually look like?"""
import requests
import re

url = "https://careers-cotiviti.icims.com/jobs/19595/vp-client-engagement/job"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Strategy 1: ?in_iframe=1
iframe_url = url + "?in_iframe=1"
r = requests.get(iframe_url, timeout=15, headers=headers)
html = r.text

print(f"=== ?in_iframe=1 response ===")
print(f"Status: {r.status_code}")
print(f"HTML length: {len(html)} chars")
print(f"Has <title>: {'<title>' in html}")
print(f"Has iCIMS_: {'iCIMS_' in html.lower()}")
print(f"Has 'icims': {'icims' in html.lower()}")

# Check for meta tags
meta_desc = re.search(r'<meta[^>]*description[^>]*>', html, re.I)
print(f"\nMeta description tag found: {bool(meta_desc)}")
if meta_desc:
    print(f"  Raw tag: {meta_desc.group(0)[:200]}")

meta_og = re.search(r'<meta[^>]*og:description[^>]*>', html, re.I)
print(f"Meta og:description tag found: {bool(meta_og)}")
if meta_og:
    print(f"  Raw tag: {meta_og.group(0)[:200]}")

# Check title
title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.DOTALL)
if title_match:
    print(f"\nTitle: {title_match.group(1)[:100]}")

# Check for iCIMS location pattern in body
loc_pat = re.search(r'(?:US|CA|GB|UK|DE|FR|IN|AU)-[A-Z]{2,3}-[\w\s]+', html)
print(f"\niCIMS location pattern found: {bool(loc_pat)}")
if loc_pat:
    print(f"  Value: {loc_pat.group(0)}")

# Print first 2000 chars of HTML to see structure
print(f"\n=== First 2000 chars ===")
print(html[:2000])
print(f"\n=== Last 500 chars ===")
print(html[-500:])
