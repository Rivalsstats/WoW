"""
Generate llm.txt file for AI usage policy and site documentation.

This script creates a comprehensive sitemap-style document that includes:
- All pages with URLs and descriptions extracted from meta tags
- Class guides grouped by role (Tank/Dps/Healer)
- Dungeon guides
- Main site sections
- Usage policy information

Output: llm.txt in the root directory
"""

import os
import re
import argparse
import html
from datetime import datetime, timezone

DOMAIN = "https://mythistone.com"
OUTPUT_FILE = "llms.txt"

# Directories containing output HTML files
SEARCH_DIRECTORIES = ["classes", "dungeons", "pages"]
ROOT_FILES = ["index.html"]

# Main site sections (non-class/dungeon pages)
MAIN_SECTIONS = [
    {"name": "Home", "path": "index.html"},
    {"name": "Dashboard", "path": "pages/dashboard.html"},
    {"name": "Routes", "path": "pages/routes.html"},
    {"name": "Composition Analysis", "path": "pages/comps.html"},
    {"name": "Blog", "path": "pages/blog.html"},
]


def extract_meta_description(html_content):
    """
    Extract meta description from HTML using regex.
    
    Args:
        html_content: The HTML content as a string
        
    Returns:
        The description content or "No description available"
    """
    pattern = r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']'
    match = re.search(pattern, html_content, re.IGNORECASE)
    if match:
        # Decode HTML entities (e.g., &#39; -> ')
        return html.unescape(match.group(1))
    return "No description available"


def extract_noindex(html_content):
    """
    Check if page has noindex meta tag.
    
    Args:
        html_content: The HTML content as a string
        
    Returns:
        True if noindex is present, False otherwise
    """
    pattern = r'<meta\s+name=["\']robots["\']\s+content=["\'][^"\']*noindex[^"\']*["\']'
    return bool(pattern.search(html_content, re.IGNORECASE))


def scan_pages_for_descriptions(directories):
    """
    Scan generated HTML files and extract descriptions.
    
    Args:
        directories: List of directory paths to scan
        
    Returns:
        List of dictionaries with page information
    """
    pages = []
    
    # Compile noindex pattern once for efficiency
    noindex_pattern = re.compile(r'<meta\s+name=["\']robots["\']\s+content=["\'][^"\']*noindex[^"\']*["\']', re.IGNORECASE)
    
    for directory in directories:
        if not os.path.exists(directory):
            continue
            
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".html"):
                    file_path = os.path.join(root, file)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                        
                        # Skip pages with noindex
                        if noindex_pattern.search(content):
                            print(f"Skipping {file_path} (noindex found)")
                            continue
                        
                        description = extract_meta_description(content)
                        pages.append({
                            "path": file_path,
                            "description": description
                        })
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
    
    return pages


def build_url_from_path(file_path):
    """
    Convert file path to URL.
    
    Args:
        file_path: The file path relative to root
        
    Returns:
        Full URL string
    """
    url_path = file_path.replace("\\", "/")  # Normalize for windows/linux
    
    if url_path == "index.html":
        return f"{DOMAIN}/"
    
    # Remove .html extension
    if url_path.endswith(".html"):
        url_path = url_path[:-5]
    
    return f"{DOMAIN}/{url_path}"


def get_role_from_path(file_path):
    """
    Extract role (Tank/Dps/Healer) from class file path.
    
    Args:
        file_path: Path to the class HTML file
        
    Returns:
        Role string or None
    """
    match = re.search(r'classes[/\\](Tank|Dps|Healer)', file_path, re.IGNORECASE)
    return match.group(1) if match else None


def get_spec_name_from_path(file_path):
    """
    Extract spec name from file path.
    
    Args:
        file_path: Path to the class HTML file
        
    Returns:
        Formatted spec name (e.g., "Arcane Mage")
    """
    # Get filename without extension
    filename = os.path.basename(file_path)
    if filename.endswith(".html"):
        filename = filename[:-5]
    
    # Convert underscores to spaces and title case
    return filename.replace("_", " ").title()


def get_dungeon_name_from_path(file_path):
    """
    Extract dungeon name from file path.
    
    Args:
        file_path: Path to the dungeon HTML file
        
    Returns:
        Formatted dungeon name
    """
    # Get filename without extension
    filename = os.path.basename(file_path)
    if filename.endswith(".html"):
        filename = filename[:-5]
    
    # Convert hyphens to spaces and title case
    return filename.replace("-", " ").title()


def group_class_pages(pages):
    """
    Group class pages by role (Tank/Dps/Healer).
    
    Args:
        pages: List of page dictionaries
        
    Returns:
        Dictionary with roles as keys and lists of pages as values
    """
    grouped = {
        "Tank": [],
        "Dps": [],
        "Healer": []
    }
    
    for page in pages:
        role = get_role_from_path(page["path"])
        if role:
            grouped[role].append(page)
    
    # Sort specs alphabetically within each role
    for role in grouped:
        grouped[role].sort(key=lambda x: get_spec_name_from_path(x["path"]))
    
    return grouped


def group_dungeon_pages(pages):
    """
    Filter and return dungeon pages.
    
    Args:
        pages: List of page dictionaries
        
    Returns:
        Sorted list of dungeon pages
    """
    dungeons = []
    
    for page in pages:
        # Normalize path separators for comparison
        normalized_path = page["path"].replace("\\", "/")
        if normalized_path.startswith("dungeons/"):
            dungeons.append(page)
    
    # Sort alphabetically
    dungeons.sort(key=lambda x: get_dungeon_name_from_path(x["path"]))
    
    return dungeons


def generate_llm_txt(output_dir="."):
    """
    Generate the llm.txt file.
    
    Args:
        output_dir: Directory where llm.txt should be written
    """
    print("Generating llm.txt...")
    
    # Get current season info from database if available
    season_name = "Current Season"
    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from databaseConnector import init_connection_pool, get_connection
        import os as os_module
        
        # Get DB config from environment
        db_host = os_module.environ.get("DATABASE_HOST")
        db_user = os_module.environ.get("DATABASE_USER")
        db_password = os_module.environ.get("DATABASE_PASSWORD")
        db_name = os_module.environ.get("DATABASE_NAME")
        db_port = os_module.environ.get("DATABASE_PORT")
        
        if all([db_host, db_user, db_password, db_name, db_port]):
            init_connection_pool(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name,
                port=int(db_port)
            )
            
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT name FROM seasons WHERE current = 1 LIMIT 1")
            result = cursor.fetchone()
            if result:
                season_name = result["name"]
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Could not fetch season info from database: {e}")
    
    # Scan all pages for descriptions
    all_pages = scan_pages_for_descriptions(SEARCH_DIRECTORIES)
    
    # Group pages
    class_pages = group_class_pages(all_pages)
    dungeon_pages = group_dungeon_pages(all_pages)
    
    # Get current timestamp
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    
    # Build output content
    lines = []
    
    # Header
    lines.append("# MythiStone - AI Usage Policy")
    lines.append("")
    
    # Description section
    index_desc = "No description available"
    if os.path.exists("index.html"):
        try:
            with open("index.html", 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            index_desc = extract_meta_description(content)
        except Exception as e:
            print(f"Could not read index.html: {e}")
    
    lines.append("## Description")
    lines.append(index_desc)
    lines.append("")
    
    # Last Updated
    lines.append("## Last Updated")
    lines.append(current_time)
    lines.append("")
    
    # Usage Policy
    lines.append("## Usage Policy")
    lines.append("- ❌ **No AI Training**: Content may NOT be used for training AI/ML models")
    lines.append("- ✅ **Quoting Allowed**: Content may be quoted with full attribution")
    lines.append("- ❌ **No Commercial Redistribution**: Commercial use requires explicit permission")
    lines.append("")
    
    # Attribution Requirements
    lines.append("## Attribution Requirements")
    lines.append("When quoting content from MythiStone, you must:")
    lines.append("1. Credit \"MythiStone\" as the source")
    lines.append("2. Link to the original page URL")
    lines.append("3. Include a link back to https://mythistone.com")
    lines.append("")
    
    # Main Site Sections
    lines.append("## Site Sections")
    lines.append("")
    
    for section in MAIN_SECTIONS:
        lines.append(f"### {section['name']}")
        
        # Try to get description from HTML if it exists
        description = "No description available"
        if os.path.exists(section["path"]):
            try:
                with open(section["path"], 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                description = extract_meta_description(content)
            except Exception as e:
                print(f"Could not read {section['path']}: {e}")
        
        url = build_url_from_path(section["path"])
        lines.append(f"- URL: {url}")
        lines.append(f"- Description: {description}")
        lines.append("")
    
    # Class Guides (grouped by role)
    lines.append("## Class Guides")
    lines.append("")
    
    for role in ["Tank", "Dps", "Healer"]:
        role_pages = class_pages.get(role, [])
        if not role_pages:
            continue
            
        lines.append(f"### {role}")
        
        for page in role_pages:
            spec_name = get_spec_name_from_path(page["path"])
            url = build_url_from_path(page["path"])
            description = page["description"]
            
            lines.append(f"#### {spec_name}")
            lines.append(f"- URL: {url}")
            lines.append(f"- Description: {description}")
            lines.append("")
    
    # Dungeon Guides
    lines.append("## Dungeon Guides")
    lines.append("")
    
    for page in dungeon_pages:
        dungeon_name = get_dungeon_name_from_path(page["path"])
        url = build_url_from_path(page["path"])
        description = page["description"]
        
        lines.append(f"### {dungeon_name}")
        lines.append(f"- URL: {url}")
        lines.append(f"- Description: {description}")
        lines.append("")
    
    # Sitemap reference
    lines.append("## Sitemap")
    lines.append(f"- URL: {DOMAIN}/sitemap.xml")
    lines.append("")
    
    # Write to file
    output_path = os.path.join(output_dir, OUTPUT_FILE)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    total_pages = sum(len(pages) for pages in class_pages.values()) + len(dungeon_pages) + len(MAIN_SECTIONS)
    print(f"llm.txt generated successfully with {total_pages} entries.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate llm.txt for AI documentation")
    parser.add_argument(
        "--output_dir",
        default=".",
        help="Output directory for llm.txt (default: current directory)"
    )
    args = parser.parse_args()
    
    generate_llm_txt(args.output_dir)