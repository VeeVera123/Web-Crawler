"""Debug: What does the raw iCIMS HTML actually look like?
Focus on finding the full JD hidden in JS/data attributes."""
import requests
import re
import json

url = "https://careers-cotiviti.icims.com/jobs/19595/vp-client-engagement/job"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Strategy 1: ?in_iframe=1
iframe_url = url + "?in_iframe=1"
r = requests.get(iframe_url, timeout=15, headers=headers)
html = r.text

print(f"HTML length: {len(html)} chars")

# Check og:description length
og_match = re.search(r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']', html, re.I)
if not og_match:
    og_match = re.search(r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:description["\']', html, re.I)
if og_match:
    print(f"\nog:description: {len(og_match.group(1))} chars")
    print(f"  First 200: {og_match.group(1)[:200]}")

# Look for JSON data in script tags (common in SPAs)
print(f"\n=== Script tags with potential JD content ===")
for i, script in enumerate(re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)):
    content = script.group(1).strip()
    if not content or len(content) < 100:
        continue
    # Check for job-related keywords
    has_job_data = any(kw in content.lower() for kw in [
        'description', 'jobdescription', 'job_description',
        'responsibilities', 'qualifications', 'requirements',
        'jobtitle', 'job_title', 'jobcontent',
    ])
    if has_job_data:
        print(f"\nScript #{i}: {len(content)} chars (has job keywords)")
        print(f"  First 300: {content[:300]}")
        # Try to parse as JSON
        try:
            data = json.loads(content)
            print(f"  Valid JSON! Keys: {list(data.keys()) if isinstance(data, dict) else 'array'}")
        except:
            pass

# Look for noscript content
noscript = re.findall(r'<noscript[^>]*>(.*?)</noscript>', html, re.DOTALL)
for i, ns in enumerate(noscript):
    if len(ns.strip()) > 50:
        print(f"\n<noscript> #{i}: {len(ns)} chars")
        print(f"  First 200: {ns[:200]}")

# Look for data attributes with content
data_attrs = re.findall(r'data-(?:description|content|job|text)=["\']([^"\']{100,})["\']', html, re.I)
for i, da in enumerate(data_attrs):
    print(f"\ndata-* attribute #{i}: {len(da)} chars")
    print(f"  First 200: {da[:200]}")

# Look for hidden divs or sections
hidden = re.findall(r'(?:style="[^"]*display:\s*none[^"]*"|hidden)[^>]*>(.*?)</(?:div|section|span)', html, re.DOTALL | re.I)
for i, h in enumerate(hidden):
    stripped = re.sub(r'<[^>]+>', ' ', h).strip()
    if len(stripped) > 100:
        print(f"\nHidden element #{i}: {len(stripped)} chars")
        print(f"  First 200: {stripped[:200]}")

# Try alternative URL: mobile version
print(f"\n\n=== Trying mobile version ===")
mobile_url = url + "?mobile=true&needsRedirect=false"
r2 = requests.get(mobile_url, timeout=15, headers=headers)
print(f"Mobile HTML length: {len(r2.text)} chars")
# Check for visible job content in mobile
for pat_name, pat in [
    ("iCIMS_JobContent", r'class="iCIMS_JobContent[^"]*"[^>]*>(.*?)</div>'),
    ("iCIMS_InfoMsg", r'class="iCIMS_InfoMsg[^"]*"[^>]*>(.*?)</div>'),
    ("job-description", r'id="job-description"[^>]*>(.*?)</div>'),
    ("additionalFields", r'class="[^"]*additionalFields[^"]*"[^>]*>(.*?)</div>'),
    ("iCIMS_MainWrapper", r'class="[^"]*iCIMS_MainWrapper[^"]*"[^>]*>(.*?)(?:</div>){2}'),
]:
    m = re.search(pat, r2.text, re.DOTALL | re.I)
    if m:
        text = re.sub(r'<[^>]+>', ' ', m.group(1)).strip()
        text = re.sub(r'\s+', ' ', text)
        print(f"\n{pat_name}: {len(text)} chars")
        print(f"  First 300: {text[:300]}")

# Try ?mode=job (some iCIMS have this)
print(f"\n\n=== Trying ?mode=job ===")
try:
    r3 = requests.get(url + "?mode=job", timeout=10, headers=headers)
    print(f"mode=job status: {r3.status_code}, length: {len(r3.text)} chars")
    if r3.headers.get('content-type', '').startswith('application/json'):
        print("  Returns JSON!")
        data = r3.json()
        print(f"  Keys: {list(data.keys()) if isinstance(data, dict) else 'type: ' + type(data).__name__}")
except Exception as e:
    print(f"  Error: {e}")
