# UI
This is a web interface for viewing the benchmark results produced by `clp-bench`.

## Requirements

+ Node.js
+ Python v3.10 or higher

## Set up
The web interface includes a backend and a frontend.

## Backend
+ Enter the `backend` directory. 
+ Create a virtual `python3` environment under `/backend`:
  ```shell
  python3 -m venv venv
  ```
+ Install dependencies:
  ```shell
  . venv/bin/activate
  pip install -r requirements.txt
  ```
+ Run the backend:
  ```shell
  python3 app.py
  ```
+ If this is the first time you've run the backend, you may also need to load the data (leave app.py
  running and run the following in another window):
  ```shell
  python3 load_results.py
  ```

## Frontend
* Enter the `frontend` directory.
+ Install dependencies:
  ```
  npm install
  ```
+ During development, you can run the frontend with:
  ```
  npm run dev
  ```
  + The command will print out the address of the web interface.
+ In production, you can build the frontend with:
  ```
  npm run build
  ```
  + The frontend will be available through the backend's address.

## Configuration
There is a template `.env` file in this directory. To create a custom configuration, you may copy
`.env` to `.env.local` and modify the content, which will override the settings in `.env`.
