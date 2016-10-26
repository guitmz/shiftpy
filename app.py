from flask import Flask, request
from flask_restful import reqparse, Resource, Api

app = Flask(__name__)
api = Api(app)
app.url_map.strict_slashes = False
parser = reqparse.RequestParser()


class RedshiftResource(Resource):
    @staticmethod
    def post():
        from redshift.base import RedshiftExporter
        data = request.json
        exporter = RedshiftExporter(**data)
        exporter.send_to_redshift()
        return 201

api.add_resource(RedshiftResource, '/send_to_redshift')

if __name__ == '__main__':
    app.run()
