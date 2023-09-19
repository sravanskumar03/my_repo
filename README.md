import boto3

ses_client = boto3.client('ses')

response = ses_client.send_email(
    Destination={
        'ToAddresses': [
            'recipient@example.com',
        ],
    },
    Message={
        'Body': {
            'Html': {
                'Charset': 'UTF-8',
                'Data': '''<html>
<head>
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
th, td {
  padding: 5px;
}
</style>
</head>
<body>
<table style="width:100%">
  <tr>
    <th>Firstname</th>
    <th>Lastname</th> 
    <th>Age</th>
  </tr>
  <tr>
    <td>John</td>
    <td>Doe</td>
    <td>30</td>
  </tr>
  <tr>
    <td>Jane</td>
    <td>Doe</td>
    <td>25</td>
  </tr>
</table>
</body>
</html>''',
            },
        },
        'Subject': {
            'Charset': 'UTF-8',
            'Data': 'HTML Table Example',
        },
    },
    Source='sender@example.com',
)

print(response)
