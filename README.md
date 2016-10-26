# shiftpy
Python restful API to help sending data to Redshift

# Running it
I recommend running it as a Docker container:

```
$ docker pull guitmz/shiftpy
  
$ docker run -d -p 5000:5000 guitmz/shiftpy

 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```

But you can also run it like this:

```
$ git clone https://github.com/guitmz/shiftpy.git && cd shiftpy

$ export FLASK_APP=app.py

$ flask run

  * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```
# Usage
```
curl -XPOST -H "Content-Type: application/json" http://127.0.0.1:5000/send_to_redshift/ -d '{"s3path":"accounts/accounts_oct_26.csv", "table_name":"accounts", "fields":["id", "name", "address"]}'
```
