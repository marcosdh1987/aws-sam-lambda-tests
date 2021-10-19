import json
import requests
import pandas as pd

# import requests


def lambda_handler(event, context):
      
    #define var
    source_url = "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.9/download/indice-precios-al-consumidor-apertura-por-categorias-base-diciembre-2016-mensual.csv"
    
    try:
        response = requests.get(source_url)
        #logger.info("URL is valid and exists on the internet")
        input_filepath = 'lambda-test/lambda_function/data/base_inflacion.txt'


        df_inflacion_prisma = pd.read_csv(input_filepath, sep=',')
        print('df_inflation_prisma.shape: {}'.format(df_inflacion_prisma.shape))
        df_inflacion_prisma.sort_values(by='periodo', inplace=True)
        df_inflacion_prisma['fecha'] = pd.to_datetime(
            df_inflacion_prisma['fecha'],
            format='%Y-%m-%d')



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


    
