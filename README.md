[![Static data collection](https://custom-icon-badges.demolab.com/github/actions/workflow/status/MythiStone/mythistone.github.io/getStaticData.yml?style=for-the-badge&logo=Mythistone&labelColor=%2322090E&label=Static%20data%20collection)](https://github.com/MythiStone/mythistone.github.io/actions/workflows/getStaticData.yml)
[![Deployment](https://custom-icon-badges.demolab.com/github/actions/workflow/status/MythiStone/mythistone.github.io/buildPages.yml?style=for-the-badge&logo=Mythistone&labelColor=%2322090E&label=Deployment)](https://github.com/MythiStone/mythistone.github.io/actions/workflows/buildPages.yml)
[![Collector Image](https://custom-icon-badges.demolab.com/github/actions/workflow/status/MythiStone/mythistone.github.io/buildCollectorImage.yml?style=for-the-badge&logo=Mythistone&labelColor=%2322090E&label=Collector%20Image)](https://github.com/MythiStone/mythistone.github.io/actions/workflows/buildCollectorImage.yml)
[![Social Media Posts](https://custom-icon-badges.demolab.com/github/actions/workflow/status/MythiStone/mythistone.github.io/automatedSocialMediaPosts.yml?style=for-the-badge&logo=Mythistone&labelColor=%2322090E&label=Social%20Media%20Posts)](https://github.com/MythiStone/mythistone.github.io/actions/workflows/automatedSocialMediaPosts.yml)
[![Patreon](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Fshieldsio-patreon.vercel.app%2Fapi%3Fusername%3DJodsderechte%26type%3Dpatrons&style=for-the-badge&labelColor=%2322090E)](https://www.patreon.com/c/Jodsderechte)
[![Discord](https://img.shields.io/badge/Discord-7289da?logo=discord&logoColor=fff&style=for-the-badge)](https://discord.gg/v3gYmYamGJ)



# Mythistone Project Overview

**MythiStone** is an interactive World of Warcraft Mythic+ dashboard that provides tier lists, leaderboards, dungeon route finding and per-spec statistics for the current season. The site combines data from the Blizzard API, Raider.IO, Keystone.guru, and community sources to help players plan and optimize Mythic+ runs. The frontend is a static website (built with Jinja2 templates) that presents dashboards, tier lists, class guides, and a team route finder in a responsive Material Dashboard theme.

# Purpose and Scope

This repository contains the source for [Mythistone](https://mythistone.com/), including data processing scripts and page templates. The primary purpose is to aggregate Mythic+ performance data (dungeon performance, spec talent popularity, gear recommendations, etc.) and present it visually in charts and tables. Key features include Mythic+ overall statistics, tier lists, per-spec gear and talent information, and a “Route Finder” tool for finding keystone routes based on team composition. The scope is limited to static content so the site can be hosted on GitHub Pages. All heavy data processing is done offline before publishing.

# Architecture Overview

- **Static Site Generation:** Pages are generated from Jinja2 templates using Python scripts. The build system pulls data from APIs and a MySQL database and applies it to templates to produce static output for hosting on GitHub Pages.

- **Frontend:** 
    - The site layout is based on the [Material Dashboard UI theme](https://www.creative-tim.com/product/material-dashboard).
    - Charts and graphs are rendered client-side using [Chart.js](https://www.chartjs.org/).
    - Client-side search and filtering enable interactive features without a backend server.
    - Interactive WoW data is driven by [Wowhead tooltips](https://www.wowhead.com/tooltips).
    - Consent for cookies and tracking is managed with the open-source [Klaro consent manager](https://klaro.org/).
    - On PC, ads are optionally hosted using Google AdSense (when cookies have been consented to). Otherwise static images are used. There are no ads on mobile.


- **Data sources:** 
    - [Raider.IO API](https://raider.io/api) for highest keys and some static data.
    - [Keystone.guru API](https://keystone.guru/api/documentation) for routes.
    - [Raidbots](https://www.raidbots.com/developers) for static data and the talent frame.
    - [Blizzard’s Battle.net API](https://community.developer.battle.net/documentation) for leaderboard, character and other game data.

- **Backend:** 
    - MySQL Database
    - Static site hosting using GitHub Pages.
    - Data Collection [docker container](https://github.com/orgs/MythiStone/packages/container/package/mythistone-collector)
    - Jinja2 page templates are rebuilt every other day by a set of Python scripts which also power different forms of data collection. See [backend_scripts](https://github.com/MythiStone/mythistone.github.io/tree/main/backend_scripts) for details.


# Local Setup:

To run the site locally, you need Python installed (tested with 3.13.3) any 3.x version should work.

Install required Python packages. See the individual workflows for an up-to-date list, then install them with:
```
pip install <PackageName>
```

To view the site directly in your browser, use Python's built-in web server. From the repository root run:

```
python -m http.server
```

## Getting the collector to run

### Prerequisites

- A MySQL database with the tables defined in [backend_scripts/database.sql](https://github.com/MythiStone/mythistone.github.io/blob/main/backend_scripts/database.sql). This will at some point be included in the Docker container, but for historical reasons it is not currently. You must also set up periodic jobs to aggregate values. That setup is out of scope here.
- Four API clients (one per region) from Blizzard: https://community.developer.battle.net/access/clients. Follow Blizzard's getting-started guide if needed: https://community.developer.battle.net/documentation/guides/getting-started.
- A Raider.IO API client: https://raider.io/settings/apps.
- A Discord webhook for monitoring the Docker container. See [Discord's webhook docs](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks) for how to set one up.
- Docker (or another container runtime that can run Docker containers) installed on your system.


### Running the collector

1. Copy [`docker-compose.yml`](https://github.com/MythiStone/mythistone.github.io/blob/main/docker-compose.yml) from the repository.
2. Copy [`.envexample`](https://github.com/MythiStone/mythistone.github.io/blob/main/.envexample) and rename it to `.env`.
3. Fill in the `.env` values.
4. Run:

    ```
    docker compose up
    ```

This will populate your database with data collected from the Blizzard API.

## Building Pages

Multiple pages are built by different Python scripts. See the build workflow to view all available pages: [here](https://github.com/MythiStone/mythistone.github.io/blob/main/.github/workflows/buildPages.yml).

Because building spec pages can take a while, the spec script can be started with flags to build a single spec for testing:

```
--debug True --spec=<Spec Id>
```

# Automated social-media posts

In addition to the frontend, MythiStone includes automated social-media posts. The system builds a static image at 18:00 UTC (see [`.github/workflows/automatedSocialMediaPosts.yml`](https://github.com/MythiStone/mythistone.github.io/blob/main/.github/workflows/automatedSocialMediaPosts.yml)). It feeds data for image generation to an LLM via OpenRouter to generate a text/tagline, then posts the result to Twitter, Discord, and Bluesky.




