#####################################
# DeployAPIs.py
#####################################
# Description:
# * Register all flask blueprints.

from Configs.ETLSummaryReportAPI import etlsummaryreport_bp
from flask import Flask

# https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3
# https://flask.palletsprojects.com/en/1.1.x/deploying/
# https://octopus.com/docs/deployment-examples/custom-scripts

app = Flask(__name__)
# Register all API blueprints (i.e. one Blueprint object per api):
app.register_blueprint(etlsummaryreport_bp)





