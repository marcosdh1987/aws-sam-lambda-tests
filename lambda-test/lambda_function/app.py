import json
import requests

# import requests


def lambda_handler(event, context):
      
    #define var
    source_url = "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.9/download/indice-precios-al-consumidor-apertura-por-categorias-base-diciembre-2016-mensual.csv"
    
    try:
        response = requests.get(source_url)
        #logger.info("URL is valid and exists on the internet")





        return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "URL is valid and exists on the internet",
            # "location": ip.text.replace("\n", "")
        }),
            }
    except:
        #logger.info("Error en URL. Verificar")
        #logger.info("Hola soy un mail")
    # mandar mail
        return {
            'statusCode': 400,
            'body': json.dumps({
            "message": "Error en URL. Verificar",
            # "location": ip.text.replace("\n", "")
        }),
        }


# sam local invoke -e ./TestFunction/lambda_event.json HelloWorldFunction
#sam local start-api


    
