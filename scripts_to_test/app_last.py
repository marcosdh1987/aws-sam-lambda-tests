import boto3
import json
import logging
import os
import pandas as pd
import requests
import sys

from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Tuple

# create logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# get client
s3_client = boto3.client('s3')


def create_inflation_data(source_url: str,df_inflacion_prisma) -> Tuple[pd.DataFrame, int]:
    """
    Execute logic that produces inflation database.

    Args:
        source_url: string with url for inflation data.

    Returns:
        df_inflacion_prisma: pandas dataframe with inflation data;
        max_i number of max months in inflation data.
    """
    
    # read url data
    base = pd.read_csv(source_url)
    df_ipc = base.loc[:, ['indice_tiempo', 'ipc_nivel_general_nacional']]

    # get month dates
    max_i = 0

    fecha_fin = datetime.today().date().replace(day=1) \
        - relativedelta(months=1)
    fecha_ini = fecha_fin - relativedelta(months=5)
    meses = pd.date_range(fecha_ini, fecha_fin, freq='MS').strftime(
        '%Y-%m-%d').tolist()

    # Borramos últimos 6 meses por si hubo un reproceso
    mes_ini = int(datetime.strptime(meses[0], '%Y-%m-%d').strftime('%Y%m'))
    mes_fin = int(datetime.strptime(meses[5], '%Y-%m-%d').strftime('%Y%m'))
    df_inflacion_prisma = df_inflacion_prisma.loc[~df_inflacion_prisma.periodo.between(mes_ini, mes_fin), :]

    logger.info('Periodos INDEC:')
    for i in range(len(meses)):
        pos_ori = df_ipc.loc[
            (df_ipc['indice_tiempo'] == meses[i]),
            'ipc_nivel_general_nacional'
        ]
        pos_ant = df_ipc.loc[
            (df_ipc['indice_tiempo'] == meses[i]).shift(-1).fillna(False),
            'ipc_nivel_general_nacional'
        ]

        if len(pos_ori) > 0:
            max_i = i
            ind_t_1_mensual = pos_ori.values[0] / pos_ant.values[0]
            per_ref = (
                datetime.strptime(meses[i], '%Y-%m-%d')
                - relativedelta(months=1)
            ).strftime('%Y-%m-%d')

            ind_t_2 = df_inflacion_prisma.loc[
                df_inflacion_prisma.fecha == per_ref,
                'indice'
            ]
            ind_t_1 = ind_t_2 * ind_t_1_mensual

            # create list with values...
            ind_t_1_df = [[
                datetime.strptime(meses[i], '%Y-%m-%d').strftime('%Y%m'),
                meses[i],
                ind_t_1.values[0],
                ind_t_1_mensual-1
            ]]

            # ... and create dataframe from the list
            ind_t_1_df = pd.DataFrame(
                ind_t_1_df,
                columns=['periodo', 'fecha', 'indice', 'var_mensual']
            )
            ind_t_1_df.fecha = pd.to_datetime(ind_t_1_df.fecha)
            logger.info("{}".format(ind_t_1_df))

            # concat
            df_inflacion_prisma = pd.concat(
                [df_inflacion_prisma, ind_t_1_df],
                axis=0,
                ignore_index=True
            )

    logger.info('Periodos estimados:')
    if max_i < 5:
        ind_pred = df_inflacion_prisma.loc[
            df_inflacion_prisma.index[-30:],
            'var_mensual'
        ].mean()

        for j in range(max_i, 5):
            per_ref = datetime.strptime(meses[j], '%Y-%m-%d')
            ind_t_2 = df_inflacion_prisma.loc[
                (df_inflacion_prisma.fecha == per_ref),
                'indice'
            ]
            ind_t_1 = ind_t_2 * (ind_pred+1)

            # create list with values...
            ind_t_1_df = [[
                datetime.strptime(meses[j+1], '%Y-%m-%d').strftime('%Y%m'),
                meses[j+1],
                ind_t_1.values[0],
                ind_pred
            ]]

            # ... and create dataframe from the list
            ind_t_1_df = pd.DataFrame(
                ind_t_1_df,
                columns=['periodo', 'fecha', 'indice', 'var_mensual']
            )
            ind_t_1_df.fecha = pd.to_datetime(ind_t_1_df.fecha)
            logger.info("{}".format(ind_t_1_df))

            # concate
            df_inflacion_prisma = pd.concat(
                [df_inflacion_prisma, ind_t_1_df],
                axis=0, ignore_index=True
            )

    return df_inflacion_prisma, max_i


def lambda_handler(event, context):
    # ---- read environment variables
    bucket_name = os.environ['BUCKET_NAME']
    key_name_intput = os.environ['INPUT_KEY_NAME']  # 'base_inflacion.txt'
    key_name_output = os.environ['OUTPUT_KEY_NAME']
    # filepath = 's3://{}/{}'.format(bucket_name, key_name_intput)
    input_filepath = '/tmp/base_inflacion_input.txt'
    source_url = os.environ['SOURCE_URL']

    # download data
    s3 = boto3.client('s3')

    # read local data
    with open(input_filepath, 'wb') as fn:
        s3.download_fileobj(bucket_name, key_name_intput, fn)

    df_inflacion_prisma = pd.read_csv(input_filepath, sep=',')
    print('df_inflation_prisma.shape: {}'.format(df_inflacion_prisma.shape))
    df_inflacion_prisma.sort_values(by='periodo', inplace=True)
    df_inflacion_prisma['fecha'] = pd.to_datetime(
        df_inflacion_prisma['fecha'],
        format='%Y-%m-%d')

    try:
        response = requests.get(source_url)
        logger.info("URL válida")
        success = True

        # create inflation table
        df_inflacion_prisma, max_i = create_inflation_data(source_url,df_inflacion_prisma)

        # write output to tmp directory and then upload to s3
        df_inflacion_prisma.to_csv(
            '/tmp/base_inflacion_output.txt',
            index=False
        )

        with open('/tmp/base_inflacion_output.txt', 'rb') as f:
            s3_client.upload_fileobj(
                Fileobj=f,
                Bucket=bucket_name,
                Key=key_name_output)

        if max_i == 0:
            logger.info("Sin info de últimos 6 meses en INDEC. Verificar.")
                # mandar mail
    
        return {
            'statusCode': 200,
            'body': ''
        }

    except:
    # except requests.exceptions.InvalidSchema as e:
    #     logger.info("Error {}. Error en la url. Verificar".format(e))
        logger.info("Error en la url. Verificar")
        success = False

        # mandar mail
        return {
            'statusCode': 400,
            'body': ''
        }
