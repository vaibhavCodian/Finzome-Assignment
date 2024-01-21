from fastapi import FastAPI, File, HTTPException,  File, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
import pandas as pd
import numpy as np
import tempfile
import os

app = FastAPI(
    title="Finzome Assignment - Financial Data Processing Api",
    description="An API for processing and analyzing financial time-series data, calculating volatility, and providing access to preprocessed data.",
    version="1.0.0",
)


@app.get("/")
def read_root_steps_to_use_api():
    """
        Root Endpoint

        Returns:
            dict: A dictionary with a simple steps to use the api.

            Steps:
                "1. ": "Upload the CSV File to /process-data",
                "2. ": "Download the process data CSV File at /get-processed-data",
                "3. ": "Delete processed data from the system make a request to /del-processed-data"
    """
    return {"Steps ": {
        "1. ": "Upload the CSV File to /process-data",
        "2. ": "Download the process data CSV File at /get-processed-data",
        "3. ": "Delete processed data from the system make a request to /del-processed-data"
    }}


@app.post("/process-data")
async def process_data(file: UploadFile = File(...)):
    """
        Process the uploaded CSV file, calculate daily and annualized volatility,
        save processed data to a new CSV file, and return processing information.

        Parameters:
        - file: UploadFile
            The CSV file to be processed.

        Returns:
        - JSONResponse
            JSON response with processing information and a message.
    """

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(file.file.read())
        file_path = temp_file.name

    # Read
    data = pd.read_csv(file_path)
    data.columns = data.columns.str.strip()

    # Data Engineering
    data['Daily Returns'] = data['Close'].pct_change()

    # Calculate Daily & Annualized Volatility
    daily_volatility = np.std(data['Daily Returns'])
    annualized_volatility = daily_volatility * np.sqrt(len(data))

    # Save processed data to a new CSV file
    output_file_path = 'output-data.csv'
    data.to_csv(output_file_path, index=False)

    # Cleanup temporary file
    temp_file.close()

    # Return response
    return JSONResponse(content={
        "message": "Data processed successfully. please request /get-data-file to fetch the preprocessed data",
        "daily_volatility": daily_volatility,
        "annualized_volatility": annualized_volatility,
        "output_csv": output_file_path
    })


@app.get("/get-processed-data")
def get_processed_data_file():
    """
        Get the preprocessed data file.

        Returns:
        - FileResponse
            Returns the preprocessed data file as a FileResponse.
   """

    file_path = "output-data.csv"
    # Check if the file exists
    if not Path(file_path).is_file():
        raise HTTPException(status_code=404, detail="Preprocessed file not found. Please preprocess the data-file "
                                                    "first before requesting, upload file at /process-data")

    return FileResponse(file_path, media_type="application/octet-stream", filename=file_path)


@app.delete("/del-processed-data")
def delete_processed_data_file():
    """
    Delete the preprocessed data file.

    Returns:
    - dict
        Returns a message indicating whether the file was deleted successfully.
    """
    file_path = "output-data.csv"

    # Check if the file exists
    if not Path(file_path).is_file():
        raise HTTPException(status_code=404, detail="Preprocessed file not found. Please preprocess the data-file "
                                                    "first before requesting upload file at /process-data")

    # Delete the file
    os.remove(file_path)

    return {"message": f"File {file_path} deleted successfully"}
