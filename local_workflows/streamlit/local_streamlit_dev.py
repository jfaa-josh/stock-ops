# TO TEST LOCALLY RUN IN POWERSHELL: streamlit run local_workflows/streamlit/local_streamlit_dev.py

import os


# Set test mode in main app
os.environ["TEST_MODE"] = "1"
os.system("streamlit run ui/streamlit_app.py")
