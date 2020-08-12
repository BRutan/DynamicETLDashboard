#####################################
# MiscControllers.py
#####################################
# Description:
# * Misc routes.

from flask import Blueprint, Flask, make_response, Response, url_for

def healthcheck():
    return Response(status = 200)



