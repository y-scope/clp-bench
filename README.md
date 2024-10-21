# Setup
## Web Setup
```
cd ui
```
### Backend Setup
First, `cd backend`. 
+ Create a virtual `python3` environment under `/backend`:
  ```
  # Since clp-bench is a python package so we only need one python environment for better management
  python3 -m venv ../../venv
  ```
+ Install dependencies:
  ```
  source ../../venv/bin/activate
  pip install -r requirements.txt
  ```
+ Run the backend:
  ```
  python3 app.py
  ```
+ You may also need to load the data if first time setup (run the following in another window  to keep the backend running):
  ```
  python3 load_results.py
  ```
### Frontend Setup
First, `cd frontend`.
+ Install dependencies:
  ```
  npm install
  ```
+ During development, you can run the frontend with:
  ```
  npm run dev
  ```
  + The command will print out the address of the site.
+ In production, you can build the frontend with:
  ```
  npm run build
  ```
  + The frontend will be available through the backend's address.
### Customization
There is a template `.env` file under the root directory, where the default settings are applied.

To create a custom configuration, you may copy `.env` to `.env.local` and modify the content, which will override the settings in `.env`.

## CLP-Bench Setup
We use `clp-bench` as the tool to run the benchmark, which is an out-of-the-box python package to run and get the benchmarking results conveniently.
```
source venv/bin/activate
pip install install -e .
clp-bench --help
```