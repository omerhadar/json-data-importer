# JSON Data Processing & Transformation Engine

## Overview
This project implements a generic, optimized, and concurrent data processing pipeline designed to read raw JSON user data, transform it into a standardized structure, and export it for database loading.

The solution allows for high-performance batch processing without reliance on heavy external frameworks like Apache Spark, adhering strictly to the assignment constraints.

---

## 🚀 Key Features

* **Generic Framework:** Driven entirely by an external configuration file (`table_config.json`), allowing the solution to adapt to different input schemas and transformation logic without code changes.
* **Parallel Concurrency:** Utilizes Python's `ProcessPoolExecutor` to parse multiple files simultaneously, maximizing CPU usage on multi-core machines.
* **Custom Batch Processing:** Implements a native file batch processing approach using standard libraries and `pandas`.
* **Data Transformation:** Normalizes nested JSON structures and flattens complex objects like `signInActivity`.

---

## ⚙️ Architecture & Design Decisions


### 1. Generic & Config-Driven Design
* **Approach:** All column mappings, renaming rules, and record paths are defined in `table_config.json`.
* **Benefit:** New data sources or changing requirements can be handled solely by updating the JSON config, keeping the Python code closed for modification but open for extension.

### 2. Concurrency Model
* **Approach:** I used `concurrent.futures.ProcessPoolExecutor` for parallelism, to give better performance over large amounts of files given the computing power, while also letting the configuration be changed according to needs.
* **Trade-off:** I chose Multiprocessing over Multithreading because Python's Global Interpreter Lock (GIL) limits true parallelism for CPU-bound tasks like JSON parsing. Multiprocessing bypasses the GIL, offering better performance for this specific workload.

### 3. Data Modeling: SignInActivity
* **Approach:** Flattening.
* **Logic:** The `signInActivity` object contains 1:1 attributes relative to the user (e.g., `lastSignInDateTime`). Instead of creating a separate relational table which would require expensive joins later, I flatten these fields directly into the main user record (e.g., `signin_activity.last_signin_datetime`) via the configuration file.

### 4. External ID Generation
* **Logic:** As per the requirement to include an `external_id`, a UUID (v4) is generated programmatically for every record during the post-processing phase to ensure global uniqueness while also allowing for others to develop any other post-processing transformations.

---

## 🛠️ Setup & Installation

### Prerequisites
* Python 3.8+
* Pandas

### Installation
1.  Clone the repository.
2.  Install the required dependencies:
    ```bash
    pip install pandas
    ```

---

## 🏃 How to Run

The application relies on environment variables to locate your input files and configuration. This ensures the code remains environment-agnostic.

1.  **Prepare your data:** Ensure your input JSON files are in a specific directory.
2.  **Set Environment Variables:**
    * `FILES_DIR`: Path to the folder containing input JSON files.
    * `TABLE_CONFIG_PATH`: Path to the `table_config.json` file.

**Linux/Mac:**
```bash
export FILES_DIR="./path/to/your/json/files"
export TABLE_CONFIG_PATH="./table_config.json"
python main.py
```

---

## ✅ Unit Testing
### **Running the Tests**

1. **Install pytest:**
```bash
pip install pytest

```


2. **Execute the test suite:**
Run the following command in the project root:
```bash
pytest test_importer.py

```



### **Test Coverage Details**

| Test Case | Description | Requirement Covered |
| --- | --- | --- |
| `test_importer_initialization` | Verifies the class correctly loads the config file and identifies input files in the directory. | Setup & Configuration |
| `test_merging_logic` | Ensures that records from multiple independent files are aggregated into a single DataFrame without data loss. | Parallel Batch Processing |
| `test_flattening_logic` | Validates that dot-notation paths in the config (e.g., `signInActivity.lastSignInDateTime`) correctly extract nested values. | Data Transformation |

