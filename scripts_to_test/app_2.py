import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

import requests

# import requests


def lambda_handler(event, context):

    source_url = "https://infra.datos.gob.ar/catalog/sspm/dataset/145/distribution/145.9/download/indice-precios-al-consumidor-apertura-por-categorias-base-diciembre-2016-mensual.csv"

    input_filepath = 'lambda-test/lambda_function/data/base_inflacion.txt'


    df_inflacion_prisma = pd.read_csv(input_filepath, sep=',')
    print('df_inflation_prisma.shape: {}'.format(df_inflacion_prisma.shape))
    df_inflacion_prisma.sort_values(by='periodo', inplace=True)
    df_inflacion_prisma['fecha'] = pd.to_datetime(
        df_inflacion_prisma['fecha'],
        format='%Y-%m-%d')

    base = pd.read_csv(source_url)


    df_ipc = base.loc[:, ['indice_tiempo', 'ipc_nivel_general_nacional']]

    print(df_ipc.head())
    # get month dates


    fecha_fin = datetime.today().date().replace(day=1) \
        - relativedelta(months=1)
    fecha_ini = fecha_fin - relativedelta(months=5)
    meses = pd.date_range(fecha_ini, fecha_fin, freq='MS').strftime(
        '%Y-%m-%d').tolist()

    # Borramos últimos 6 meses por si hubo un reproceso
    mes_ini = int(datetime.strptime(meses[0], '%Y-%m-%d').strftime('%Y%m'))
    mes_fin = int(datetime.strptime(meses[5], '%Y-%m-%d').strftime('%Y%m'))
    df_inflacion_prisma = df_inflacion_prisma.loc[
        ~df_inflacion_prisma.periodo.between(mes_ini, mes_fin), :]

    print('Periodos INDEC:')
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
            print("{}".format(ind_t_1_df))

            # concat
            df_inflacion_prisma = pd.concat(
                [df_inflacion_prisma, ind_t_1_df],
                axis=0,
                ignore_index=True
            )

    print('Periodos estimados:')
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
            print("{}".format(ind_t_1_df))

            # concate
            df_inflacion_prisma = pd.concat(
                [df_inflacion_prisma, ind_t_1_df],
                axis=0, ignore_index=True
            )

    # write output to tmp directory and then upload to s3
    #df_inflacion_prisma.to_csv('/home/marcos/anaconda3/resources/Notebooks/ML/base_inflacion_output.txt', index=False)
    return {
                'statusCode': 400,
                'body': json.dumps({
                "message": "Error en URL. Verificar",
                # "location": ip.text.replace("\n", "")
            }),
            }