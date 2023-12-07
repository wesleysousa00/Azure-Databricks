# Generate a short-live token - Use this to generate a Databricks PAT
%sh
curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
-d 'grant_type=client_credentials' \
-d 'client_id=<CLIENT_ID_DO_APLICATIVO>' \
-d 'resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d' \
-d 'client_secret=<SEGREDO_DO_APLICATIVO>' \
https://login.microsoftonline.com/<TENANT_ID>/oauth2/token



# Create Token for ServicePrincipal
%sh
curl -X POST \
https://adb-<SEU_WORKSPACE>.azuredatabricks.net/api/2.0/token/create \
--header "Content-Type: application/json" \
--header "Authorization: Bearer <TOKEN_CRIADO_NO_CÓDIGO_ANTERIOR>" \
--data '{"application_id": "<CLIENT_ID_DO_APLICATIVO>",
         "comment": "<COMENTÁRIO SOBRE O TOKEN>",
         "lifetime_seconds": <DURACAO_DO_TOKEN_EM_SEGUNDOS>}'



# Teste Token use List ServicePrincipal
%sh
curl -X GET \
https://adb-<SEU_WORKSPACE>.azuredatabricks.net/api/2.0/preview/scim/v2/ServicePrincipals \
--header "Authorization: Bearer <TOKEN_DA_SERVICEPRINCIPAL>"
