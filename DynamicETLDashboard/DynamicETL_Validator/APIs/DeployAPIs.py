#####################################
# DeployAPIs.py
#####################################
# Description:
# * Register all flask blueprints.

from Configs.ETLSummaryReportAPI import etlsummaryreport_bp
from flask import Flask

# https://levelup.gitconnected.com/python-dependency-injection-with-flask-injector-50773d451a32
# https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3
# https://flask.palletsprojects.com/en/1.1.x/deploying/
# https://octopus.com/docs/deployment-examples/custom-scripts

flask_app = Flask(__name__)
# Register all API blueprints (i.e. one Blueprint object per api):
flask_app.register_blueprint(etlsummaryreport_bp)





