# You can Retrieve Shares with the api using the person id:

# curl  -X GET \
#    -H "Authorization:Bearer token<>" \
#  'https://api.linkedin.com/v2/shares?q=owners&owners=urn:li:person:{id}'
# You can retrieve the person id with the Retrieve Authenticated Member's Profile API

#  curl  -X GET \
#    -H "Authorization:Bearer <token>" \
#  'https://api.linkedin.com/v2/me?projection=(id)'