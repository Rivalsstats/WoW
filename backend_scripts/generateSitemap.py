import os
import re
from datetime import datetime

DOMAIN = "https://mythistone.com"
SITEMAP_FILE = "sitemap.xml"

# Directories containing output html files
SEARCH_DIRECTORIES = ["classes", "dungeons", "items", "pages"]
ROOT_FILES = ["index.html"]

def generate_sitemap():
    print("Generating sitemap.xml...")
    urls = []
    # format required by sitemap standard
    from datetime import timezone
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    
    files_to_check = []
    
    # Add root files
    for root_file in ROOT_FILES:
        if os.path.exists(root_file):
            files_to_check.append(root_file)
            
    # Traverse directories
    for directory in SEARCH_DIRECTORIES:
        if not os.path.exists(directory):
            continue
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".html"):
                    files_to_check.append(os.path.join(root, file))
                    
    # noindex meta tag regex Pattern
    noindex_pattern = re.compile(r'<meta\s+name=["\']robots["\']\s+content=["\'][^"\']*noindex[^"\']*["\']', re.IGNORECASE)

    for file_path in files_to_check:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Exclude known error pages or noindex matches
            if '404.html' in file_path or noindex_pattern.search(content):
                print(f"Skipping {file_path} (noindex found)")
                continue
                
            # Convert path to url
            url_path = file_path.replace("\\", "/") # Normalize for windows/linux
            
            if url_path == "index.html":
                loc = f"{DOMAIN}/"
                priority = "1.00"
            else:
                # Remove the '.html' suffix dynamically for URLs if that's the canonical path structure
                if url_path.endswith(".html"):
                    url_path = url_path[:-5]
                loc = f"{DOMAIN}/{url_path}"
                
                # Priority heuristical calculation 
                if url_path.startswith("pages/"):
                    priority = "0.90"
                elif url_path.startswith("classes/"):
                    priority = "0.80"
                elif url_path.startswith("dungeons/"):
                    priority = "0.80"
                elif url_path.startswith("items/"):
                    priority = "0.70"
                else:
                    priority = "0.80"
                    
            urls.append({
                "loc": loc.replace(" ", "%20"), # Escape URL spaces
                "lastmod": current_time,
                "priority": priority
            })
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Write to sitemap.xml
    with open(SITEMAP_FILE, "w", encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset\n')
        f.write('  xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n')
        f.write('  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n')
        f.write('  xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">\n')
        f.write('  <!-- dynamically generated sitemap -->\n\n')
        
        for url in urls:
            f.write("  <url>\n")
            f.write(f"    <loc>{url['loc']}</loc>\n")
            f.write(f"    <lastmod>{url['lastmod']}</lastmod>\n")
            f.write(f"    <priority>{url['priority']}</priority>\n")
            f.write("  </url>\n")
            
        f.write("</urlset>\n")
    print(f"Sitemap generated successfully with {len(urls)} entries.")

if __name__ == "__main__":
    generate_sitemap()
