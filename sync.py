from pymongo import MongoClient
from bson.objectid import ObjectId
import requests, urllib3, json, time

#Main_Client = MongoClient('mongodb://localhost:27017')
#Main_Client = MongoClient('mongodb://admin:admin@mongodb-36-rhel7.edge-satellite-main.svc.cluster.local:27017')
#Main_Client = MongoClient('mongodb://admin:admin@main-bankdb.test-open-banking.svc:27017')
Main_Client = MongoClient('mongodb://admin:admin@main-bankdb.edge-open-banking-tcs.svc:27017')
Main_BankDB = Main_Client['Main_BankDB']

Main_Accounts = Main_BankDB['Main_Accounts']
Main_Dormant_Accounts = Main_BankDB['Main_Dormant_Accounts']
Main_Cheque_Books = Main_BankDB['Main_Cheque_Books']
Main_Credit_Cards = Main_BankDB['Main_Credit_Cards']

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

accessToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik5UZG1aak00WkRrM05qWTBZemM1TW1abU9EZ3dNVEUzTVdZd05ERTVNV1JsWkRnNE56YzRaQT09In0.eyJhdWQiOiJodHRwOlwvXC9vcmcud3NvMi5hcGltZ3RcL2dhdGV3YXkiLCJzdWIiOiJhZG1pbkBjYXJib24uc3VwZXIiLCJhcHBsaWNhdGlvbiI6eyJvd25lciI6ImFkbWluIiwidGllclF1b3RhVHlwZSI6InJlcXVlc3RDb3VudCIsInRpZXIiOiIxMFBlck1pbiIsIm5hbWUiOiJ0ZXN0X3N5bmMiLCJpZCI6MywidXVpZCI6bnVsbH0sInNjb3BlIjoiYW1fYXBwbGljYXRpb25fc2NvcGUgZGVmYXVsdCIsImlzcyI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6OTQ0M1wvb2F1dGgyXC90b2tlbiIsInRpZXJJbmZvIjp7IkdvbGQiOnsidGllclF1b3RhVHlwZSI6InJlcXVlc3RDb3VudCIsInN0b3BPblF1b3RhUmVhY2giOnRydWUsInNwaWtlQXJyZXN0TGltaXQiOjAsInNwaWtlQXJyZXN0VW5pdCI6bnVsbH19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6ImdldF91bnByb2Nlc3NlZF9hY2NvdW50IiwiY29udGV4dCI6IlwvZ2V0X3VucHJvY2Vzc2VkX2FjY291bnRcLzEuMCIsInB1Ymxpc2hlciI6ImFkbWluIiwidmVyc2lvbiI6IjEuMCIsInN1YnNjcmlwdGlvblRpZXIiOiJHb2xkIn0seyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6InVwZGF0ZV9lZGdlZGJfZnJvbV9tYWluZGIiLCJjb250ZXh0IjoiXC91cGRhdGVfZWRnZWRiX2Zyb21fbWFpbmRiXC8xLjAiLCJwdWJsaXNoZXIiOiJhZG1pbiIsInZlcnNpb24iOiIxLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoiR29sZCJ9LHsic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJnZXRfdW5wcm9jZXNzZWRfZG9ybWFudF9hY250IiwiY29udGV4dCI6IlwvZ2V0X3VucHJvY2Vzc2VkX2Rvcm1hbnRfYWNudFwvMS4wIiwicHVibGlzaGVyIjoiYWRtaW4iLCJ2ZXJzaW9uIjoiMS4wIiwic3Vic2NyaXB0aW9uVGllciI6IkdvbGQifSx7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoidXBkYXRlX2VkZ2VfZG9ybWFudF9mcm9tX21haW4iLCJjb250ZXh0IjoiXC91cGRhdGVfZWRnZV9kb3JtYW50X2Zyb21fbWFpblwvMS4wIiwicHVibGlzaGVyIjoiYWRtaW4iLCJ2ZXJzaW9uIjoiMS4wIiwic3Vic2NyaXB0aW9uVGllciI6IkdvbGQifSx7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoiZ2V0X3VucHJvY2Vzc2VkX2NoZXF1ZV9ib29rcyIsImNvbnRleHQiOiJcL2dldF91bnByb2Nlc3NlZF9jaGVxdWVfYm9va3NcLzEuMCIsInB1Ymxpc2hlciI6ImFkbWluIiwidmVyc2lvbiI6IjEuMCIsInN1YnNjcmlwdGlvblRpZXIiOiJHb2xkIn0seyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6InVwZGF0ZV9lZGdlX2NoZXF1ZV9mcm9tX21haW4iLCJjb250ZXh0IjoiXC91cGRhdGVfZWRnZV9jaGVxdWVfZnJvbV9tYWluXC8xLjAiLCJwdWJsaXNoZXIiOiJhZG1pbiIsInZlcnNpb24iOiIxLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoiR29sZCJ9LHsic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJnZXRfdW5wcm9jZXNzZWRfY3JlZGl0X2NhcmRzIiwiY29udGV4dCI6IlwvZ2V0X3VucHJvY2Vzc2VkX2NyZWRpdF9jYXJkc1wvMS4wIiwicHVibGlzaGVyIjoiYWRtaW4iLCJ2ZXJzaW9uIjoiMS4wIiwic3Vic2NyaXB0aW9uVGllciI6IkdvbGQifSx7InN1YnNjcmliZXJUZW5hbnREb21haW4iOiJjYXJib24uc3VwZXIiLCJuYW1lIjoidXBkYXRlX2VkZ2VfY2NhcmRfZnJvbV9tYWluIiwiY29udGV4dCI6IlwvdXBkYXRlX2VkZ2VfY2NhcmRfZnJvbV9tYWluXC8xLjAiLCJwdWJsaXNoZXIiOiJhZG1pbiIsInZlcnNpb24iOiIxLjAiLCJzdWJzY3JpcHRpb25UaWVyIjoiR29sZCJ9XSwiY29uc3VtZXJLZXkiOiJsUWp4Sm8wMnNkTnNudmZabGpERWJ6SUdROUFhIiwiZXhwIjozNzQ5NzA1OTM2LCJpYXQiOjE2MDIyMjIyODksImp0aSI6ImRmNDAxYWFjLWYwMTEtNDExNi05ZTUyLTlhY2FhY2UyMzE0NiJ9.It0PAiyBQMa4l2m9AATseIEmlRGGZ5Yl90fbqXtzPN_jGoO7MpTZj53UI45KPCObWiBSoRp4R2BlDmvOfpStantROcjja55jeK2ba8mUcbLg33RhvGR0kdFL02o0y4ATEAobYd_yTwa5iV47PQ0wE5hbPh-0bfEL4pXTIh3Y1lSjjGeMjgrAUzeatD2psH1V2QyMnFc1dpIZ-l8xQnwlH0P5Gi2nTYKC3NrDS96e48uUx-q_OITRfzZL4sXN6eYqqNPeOwEKg4tUJlRc5Npg2YL8f3ugNpoW19gk38OD5mNbk8cVYAdeRCXIMM18E7LuKvbkBQzw8GPDz5OTJuQInw"

headers = {"Authorization" : "Bearer " + accessToken}

'''
url1 = "https://158.176.180.100:8243/get_unprocessed_account/1.0"
url2 = "https://158.176.180.100:8243/update_edgedb_from_maindb/1.0"
url3 = "https://158.176.180.100:8243/get_unprocessed_dormant_acnt/1.0"
url4 = "https://158.176.180.100:8243/update_edge_dormant_from_main/1.0"
url5 = "https://158.176.180.100:8243/get_unprocessed_cheque_books/1.0"
url6 = "https://158.176.180.100:8243/update_edge_cheque_from_main/1.0"
url7 = "https://158.176.180.100:8243/get_unprocessed_credit_cards/1.0"
url8 = "https://158.176.180.100:8243/update_edge_ccard_from_main/1.0"

api_url = "http://edge-open-banking-api-ibm-cloud-edge-banking-app.satellite-cluster-6fb0b86391cd68c8282858623a1dddff-0000.upi.containers.appdomain.cloud"
'''
#api_url = "http://edge-open-api-fs-cloud-app-test-open-banking.mgmt-pot01-cluster-1fa025a294811d2b43b68d6ffd4c8b58-i000.us-east.containers.appdomain.cloud"
#api_url = "http://172.21.91.136:5000"
#api_url = "http://edge-open-api-fs-cloud-app.test-open-banking:5000"
api_url = "http://172.21.147.98:5000"

url1 = api_url+"/get_unprocessed_account"
url2 = api_url+"/update_edgedb_from_maindb"
url3 = api_url+"/get_unprocessed_dormant_acnt"
url4 = api_url+"/update_edge_dormant_from_main"
url5 = api_url+"/get_unprocessed_cheque_books"
url6 = api_url+"/update_edge_cheque_from_main"
url7 = api_url+"/get_unprocessed_credit_cards"
url8 = api_url+"/update_edge_ccard_from_main"

while(True):
    try:
        #SYNCING NEW BANK ACCOUNTS        
        while(True):
            response = requests.get(url1, headers=headers, verify=False)
            response_data = json.loads(response.text)
            print(type(response_data))
            print(response_data)

            if(int(response_data['number_of_unprocessed']) == 0):
                print(response_data['number_of_unprocessed'])
                break

            if(Main_Accounts.count_documents({"_id" : ObjectId(response_data['_id'])}) == 0):
                del response_data['number_of_unprocessed']
                response_data['_id'] = ObjectId(response_data['_id'])
                Main_Accounts.insert_one(response_data)

            body = {"objId" : str(response_data.get('_id'))}
            response = requests.post(url2, headers=headers, json=body, verify=False)
        
        #SYNCING DORMANT BANK ACCOUNTS
        while(True):
            response = requests.get(url3, headers=headers, verify=False)
            response_data = json.loads(response.text)
            print(type(response_data))
            print(response_data)

            if(int(response_data['number_of_unprocessed']) == 0):
                print(response_data['number_of_unprocessed'])
                break

            if(Main_Dormant_Accounts.count_documents({"_id" : ObjectId(response_data['_id'])}) == 0):
                del response_data['number_of_unprocessed']
                response_data['_id'] = ObjectId(response_data['_id'])
                Main_Dormant_Accounts.insert_one(response_data)

            body = {"objId" : str(response_data.get('_id'))}
            response = requests.post(url4, headers=headers, json=body, verify=False)
        
        #SYNCING CHEQUE BOOK REQUESTS
        while(True):
            response = requests.get(url5, headers=headers, verify=False)
            response_data = json.loads(response.text)
            print(type(response_data))
            print(response_data)

            if(int(response_data['number_of_unprocessed']) == 0):
                print(response_data['number_of_unprocessed'])
                break

            if(Main_Cheque_Books.count_documents({"_id" : ObjectId(response_data['_id'])}) == 0):
                del response_data['number_of_unprocessed']
                response_data['_id'] = ObjectId(response_data['_id'])
                Main_Cheque_Books.insert_one(response_data)

            body = {"objId" : str(response_data.get('_id'))}
            response = requests.post(url6, headers=headers, json=body, verify=False)

        #SYNCING CREDIT CARD REQUESTS
        while(True):
            response = requests.get(url7, headers=headers, verify=False)
            response_data = json.loads(response.text)
            print(type(response_data))
            print(response_data)

            if(int(response_data['number_of_unprocessed']) == 0):
                print(response_data['number_of_unprocessed'])
                break

            if(Main_Credit_Cards.count_documents({"_id" : ObjectId(response_data['_id'])}) == 0):
                del response_data['number_of_unprocessed']
                response_data['_id'] = ObjectId(response_data['_id'])
                Main_Credit_Cards.insert_one(response_data)

            body = {"objId" : str(response_data.get('_id'))}
            response = requests.post(url8, headers=headers, json=body, verify=False)

    except Exception as e:
        print(e)
        print("Cannot connect to edge server !!!")
    time.sleep(300)
