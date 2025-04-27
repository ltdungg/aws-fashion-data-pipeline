import json
import base64

pre = "ZXlKMWMyVnlYMmxrSWpvZ09EQTBMQ0FpZEdsdFpYTjBZVzF3SWpvZ0lqSXdNalV0TURRdE1qY2dNRFk2TlRFNk5EWWlMQ0FpWlhabGJuUmZkSGx3WlNJNklDSndZV2RsWDNacFpYY2lMQ0FpY0hKdlpIVmpkRjlwWkNJNklHNTFiR3g5"

record_data = base64.b64decode(pre).decode('utf-8')
data = base64.b64decode(record_data).decode()
print(json.loads(data))