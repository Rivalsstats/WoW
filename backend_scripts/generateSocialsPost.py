"""CLI shim kept so .github/workflows/automatedSocialMediaPosts.yml can keep
calling `python -u backend_scripts/generateSocialsPost.py --api-key ... --url ...`.

All logic lives in the social_posts package (text generation, post selection,
persistence) and the image_generation package (rendering).
"""

from social_posts.pipeline import main

if __name__ == "__main__":
    main()
