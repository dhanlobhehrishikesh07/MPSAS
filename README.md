# Multi Portable Server Architecture System (MPSAS) - Phase 2

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-GPLv3-green)
![Status](https://img.shields.io/badge/status-Phase_2_Active-orange)

MPSAS is a distributed, real-time file synchronization architecture designed to mirror project directories across multiple physical servers. It operates without a centralized, heavy database server by utilizing **Firebase Realtime Database** as a lightweight Control Bus and a **Google Drive 15GB bucket** as a temporary "Cloud RAM" buffer for binary data movement.

## 🧠 System Architecture (The 3-Index System)

Unlike traditional file-syncing tools that scan entire directories or rely on heavy SQL databases, MPSAS uses a highly optimized, flat JSON mapping system.

1. **Inventory Index (The Memory Map):** A flat, UUID-keyed JSON structure tracking all files and folders. It uses a `parent_id` system, meaning moving a 10GB folder is instantly achieved by updating a single text string, rather than moving physical bits.
2. **Journal Index (The Signal Bus):** A sequential log of changes (CREATE, MOVE, COPY, DELETE) managed by a Global Sequence ID (`gsid`). Offline servers use this to "replay" missed events and catch up instantly upon reconnecting.
3. **Transfer Index (The I/O Bridge):** Used strictly for `CREATE` operations. Large files are shredded into 10MB chunks and tracked here while they buffer through Google Drive, allowing for parallel uploading and random-access `seek()` downloading.

### 🛡️ Automated Janitor (Resource Management)
To prevent the 15GB Cloud RAM from overflowing, a multi-threaded Janitor monitors all active nodes. Only when a consensus is reached (all active servers report `status: COMPLETED`), the system purges the binary chunks from Google Drive and prunes the Journal Index.

## ⚙️ Prerequisites

*   Python 3.10 or higher.
*   A Firebase Realtime Database Service Account Key (`serviceAccountKey.json`).
*   Google Drive API Desktop Credentials (`client_secrets.json`).

## 🚀 Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YourUsername/MPSAS.git](https://github.com/YourUsername/MPSAS.git)
   cd MPSAS
