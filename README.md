# MoviesAPI Example

## Instructions

1. Create a new Python virtual environment and install dependencies

```
$ python -m venv venv
$ . venv/bin/activate       # For *nix
$ venv/Scripts/activate.bat # For Windows
$ pip install -r requirements.txt
```

2. Download and extract the data

```
$ wget https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip
$ unzip the-movies-dataset.zip movies_metadata.csv
```

4. Build the database

```
$ python makedb.py
```

5. Run the API server

```
$ uvicorn app:app
```

NOTE: FastAPI automatically generates Swagger UI documentation. This can be found at http://localhost:8000/docs once the server is running.
