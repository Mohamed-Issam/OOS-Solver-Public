# 🛠️ P6 OOS Solver 1.0

**P6 OOS Solver** is a Python-based desktop tool that connects to your local Primavera P6 database to identify and help resolve **out-of-sequence activities** efficiently.

![Tool Screenshot](https://github.com/user-attachments/assets/dc385f78-70ee-4bc4-9dfc-2b1228458a46)

---

## 🚀 Features

- Connects to a local P6 SQLite database file
- Allows project selection from available P6 projects
- Automatically analyzes and detects out-of-sequence activities
- Displays detailed progress while running
- Exports results in P6-compatible Excel format
- Saves time in resolving OOS issues manually in P6

---

## 🧭 How It Works

1. **Select Database File**  
   Prompts the user to select their local P6 `.db` file (usually found in the Documents folder).

2. **Choose a Project**  
   Displays a list of projects to choose from based on the selected database.

3. **Process OOS Activities**  
   After selecting a project, click **"Process"**. The tool will:
   - Analyze the schedule for OOS issues
   - Print real-time progress and activity details
   - Populate a table with detected issues and relationships (in P6 import format)

4. **Export to Excel**  
   Click **"Export"** to save the analysis results to an Excel file.
   - Review the output
   - Import it directly into Primavera P6 to resolve the OOS activities

---

![How-to Guide](https://github.com/user-attachments/assets/872e7ad4-cee6-4ebf-abec-3c3d02320d57)

---

## 📘 Based On

This tool solves OOS according to this post [LinkedIn guide by Eng. Amr Morsy](https://www.linkedin.com/pulse/how-solve-out-sequence-activities-amr-morsy/)

---

## 🏢 Developed By

**Mohamed Issam Gaballah**  
Version 1.0.0 | © 2025 All rights reserved.

---

## 📩 Feedback & Contributions

Have suggestions or issues? Feel free to open an [issue](https://github.com/your-repo/issues/) or reach out on LinkedIn.
[Mohamed Issam](https://www.linkedin.com/in/mohamed-issam-379186203/)
