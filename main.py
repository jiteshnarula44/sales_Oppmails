import pandas as pd
from azure.storage.blob import BlobServiceClient
import requests
from datetime import datetime
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

app = FastAPI()

# Configuration
connection_string = 'DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net'
container_name = 'intermediate'
logic_app_url = 'https://prod-30.northcentralus.logic.azure.com:443/workflows/3b6f71f697e74fa09220c7a5fcc97462/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=brM2VA8ivnivrSz5hBqS8bcfUZgDwOSciIKaipL_4Ys'

# Use async1 lifespan context manager to initialize BlobServiceClient
@asynccontextmanager
async def lifespan(app: FastAPI):
    global blob_service_client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/process-sales-reps")
def process_sales_reps():
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob='External Rep Agency Distribution Emails.csv')
        
        with open('External Rep Agency Distribution Emails.csv', 'wb') as download_file:
            download_file.write(blob_client.download_blob().readall())
        
        email_df = pd.read_csv('External Rep Agency Distribution Emails.csv')

        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%B")
        directory_path = f'{year}/{month}/' 

        blob_list = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=directory_path)
        sales_rep_files = [blob.name for blob in blob_list if blob.name.endswith('.xlsx')]

        for _, row in email_df.iterrows():
            sales_rep_name = row.iloc[0]
            matched = False

            for file_name in sales_rep_files:
                base_sales_rep_name = file_name.split('/')[-1].split('_')[0]  

                if base_sales_rep_name == sales_rep_name:
                    matched = True
                    sales_rep_email = row.iloc[1]
                    internal_sales_rep_email = row.iloc[3]
                    
                    data = {
                        'sales_rep_name': sales_rep_name,
                        'sales_rep_email': sales_rep_email,
                        'internal_sales_rep_email': internal_sales_rep_email,
                        'file_name': file_name  
                    }

                    try:
                        response = requests.post(logic_app_url, json=data)
                        if response.status_code == 200:
                            print(f'Successfully sent data for {sales_rep_name} with file {file_name}')
                    except Exception as e:
                        print(f'Error sending data for {sales_rep_name}: {e}')
            
            if not matched:
                print(f"No matching file found for {sales_rep_name}")

        return {"message": "Process completed successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Start FastAPI with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
