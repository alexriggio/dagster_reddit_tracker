import os
import json

from dagster import RunRequest, SensorResult, sensor, SensorEvaluationContext

from ..jobs import generate_pdf_report_job


@sensor(
    job=generate_pdf_report_job
)
def generate_pdf_reports_sensor(context: SensorEvaluationContext):
    """
    A Dagster sensor that monitors a directory for new or updated JSON summary files and triggers 
    the `generate_pdf_report_job` when changes are detected.

    This sensor works by:
    1. Monitoring a specified directory for JSON files.
    2. Checking if a file is new or has been modified since the last run.
    3. If changes are detected, it queues a run request for the `generate_pdf_reports` asset.

    Args:
        context (SensorEvaluationContext): Provides metadata about the sensor execution, including
                                           the previous state (cursor) and allows queuing run requests.

    Returns:
        SensorResult: Contains the run requests and the updated cursor state.
    """
    
    # Paths to directory containing summaries
    PATH_TO_SUMMARIES = os.path.join(os.path.dirname(__file__), "../../", "data/outputs/summaries")

    # Load the previous state (cursor) if it exists, otherwise initialize an empty dictionary
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    
    runs_to_request = []  # List to hold RunRequest objects

     # Iterate through all files in the summaries directory
    for filename in os.listdir(PATH_TO_SUMMARIES):
        file_path = os.path.join(PATH_TO_SUMMARIES, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):
            last_modified = os.path.getmtime(file_path)  # Get the last modified time of the file

            current_state[filename] = last_modified  # Update the current state

            # If the file is new or modified since the last run, queue it for processing
            if filename not in previous_state or previous_state[filename] != last_modified:
                with open(file_path, "r") as f:
                    summaries_json = json.load(f)  # Load the JSON content of the file

                    # Create a RunRequest for the `generate_pdf_reports` asset
                    runs_to_request.append(RunRequest(
                        run_key=f"generate_pdf_reports_{filename}_{last_modified}",  # Unique identifier for this run
                        run_config={
                            "ops": {  # ops is the standard key used in run_config to configure either an operation (op) or an asset in Dagster
                                "generate_pdf_reports": { # Configuration for the `generate_pdf_reports` asset
                                    "config": {
                                        "filename": filename,  # Pass the filename to the asset
                                        "summaries": summaries_json,  # Pass the JSON content to the asset
                                    }
                                }
                            }
                        }
                    ))
    
    # Return the run requests along with the updated cursor state
    return SensorResult(
        run_requests=runs_to_request,  # List of RunRequest objects
        cursor=json.dumps(current_state)   # Serialize the current state to JSON
    )