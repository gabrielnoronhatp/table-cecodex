import re
import oracledb
import logging
import pandas as pd  


logger = logging.getLogger('OracleInsertsLogger')
logger.setLevel(logging.DEBUG)


success_handler = logging.FileHandler('insertion_success.log')
success_handler.setLevel(logging.INFO)
success_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
success_handler.setFormatter(success_formatter)


error_handler = logging.FileHandler('insertion_errors.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)


duplicate_handler = logging.FileHandler('duplicate_records.log')
duplicate_handler.setLevel(logging.WARNING)
duplicate_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
duplicate_handler.setFormatter(duplicate_formatter)


logger.addHandler(success_handler)
logger.addHandler(error_handler)
logger.addHandler(duplicate_handler)

oracledb.init_oracle_client(lib_dir=r"C:\Users\gabriel.noronha\Downloads\instantclient-basic-windows.x64-23.6.0.24.10\instantclient_23_6")


un = 'consulta'
cs = '//10.2.1.194:1521/TPJPRD'
pw = 'consultawint'


sql = """
SELECT 
    R1.INTEGRADORA AS "COD_INTEGRADORA",
    R1.STATUSORD,
    R1.DESCSTATUSORD,
    R1.IDNOTAAPI,
    R1.DTGERRET AS "DATA_RETORNO",
    R1.LOGSTATUSORD
FROM PCPEDRETORNO R1
WHERE TRUNC(R1.DTGERRET) BETWEEN TO_DATE('01/12/2024', 'DD/MM/YYYY') AND TO_DATE('19/12/2024', 'DD/MM/YYYY')
AND R1.NUMTRANSVENDA IN (133999187, 133999842, 133999636, 133999325)
AND R1.INTEGRADORA = 3723
AND R1.STATUSORD = 'NR'
"""

try:
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        logger.info("Conexão bem-sucedida!")
        with connection.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            logger.info(f"Consulta SQL executada com sucesso, {len(results)} registros retornados.")

            extraction_data = []  

            for row in results:
                log_data = ', '.join(str(value) for value in row)
                logger.debug(f"Dados da linha: {log_data}")

            
                ean_pattern = r'ean:"(\d+)"'
                quantity_pattern = r'ordered_quantity:(\d+)'
                discount_pattern = r'wholesaler_discount:(\d+\.\d{2})'

                ean_match = re.search(ean_pattern, log_data)
                quantity_match = re.search(quantity_pattern, log_data)
                discount_match = re.search(discount_pattern, log_data)

                ean = ean_match.group(1) if ean_match else None
                quantity = float(quantity_match.group(1)) if quantity_match else None
                discount = float(discount_match.group(1)) if discount_match else None
                value_discount = quantity * discount if quantity and discount else None
                commercial_condition = row[1] 


              

                logger.info(f"Condicional comercial determinada: {commercial_condition}")

   
                data = {
                    'Order ID': row[0],
                    'Invoice ID': row[3],
                    'EAN': ean,
                    'Response Quantity': quantity,
                    'Discount Percentage': discount,
                    'Return Date': row[4],
                    'Commercial Condition': commercial_condition
                }

                
                extraction_data.append(data) 

           
                with open('extraction_log.txt', 'a') as extraction_file:
                    extraction_file.write(f"Order ID: {data['Order ID']}\n")
                    extraction_file.write(f"Invoice ID: {data['Invoice ID']}\n")
                    extraction_file.write(f"EAN: {data['EAN']}\n")
                    extraction_file.write(f"Response Quantity: {data['Response Quantity']}\n")
                    extraction_file.write(f"Discount Percentage: {data['Discount Percentage']}\n")
                    extraction_file.write(f"Return Date: {data['Return Date']}\n")
                    extraction_file.write(f"Commercial Condition: {data['Commercial Condition']}\n")
                    extraction_file.write("\n")

       
                if ean and quantity and discount:
                    try:
                      
                        check_sql = """
                        SELECT COUNT(*) FROM TAPAJOS.TAP_RETORNO_CANAL 
                        WHERE IDPEDIDOAPI = :invoice_id 
                        AND CONDICAO_COMERCIAL = :commercial_condition 
                        AND EAN = :ean
                        """
                        cursor.execute(check_sql, {
                            'invoice_id': row[3],
                            'commercial_condition': commercial_condition,
                            'ean': int(ean)
                        })
                        exists = cursor.fetchone()[0] > 0

                    
                        if not exists:
                            insert_sql = """
                            INSERT INTO TAPAJOS.TAP_RETORNO_CANAL 
                            (IDPEDIDOAPI, CONDICAO_COMERCIAL, EAN, QT, PERCDESC, VLDESCONTO)
                            VALUES (:invoice_id, :commercial_condition, :ean, :quantity, :discount, :value_discount)
                            """
                            cursor.execute(insert_sql, {
                                 'invoice_id': row[3],
                                 'commercial_condition': commercial_condition,
                                 'ean': int(ean),
                                 'quantity': quantity,
                                 'discount': discount,
                                 'value_discount': value_discount
                            })
                            logger.info(f"Dados inseridos: Invoice ID: {row[3]}, EAN: {ean}")
                        else:
                            logger.warning(f"Registro já existe: Invoice ID: {row[3]}, EAN: {ean}")
                    except oracledb.DatabaseError as db_err:
                        logger.error(f"Erro ao inserir dados: {db_err}")
                else:
                    logger.warning("Dados inválidos para inserção: EAN, quantidade ou desconto ausentes.")

       
            connection.commit()
            logger.info("Transações concluídas com sucesso.")

           
            df = pd.DataFrame(extraction_data)  
            df.to_excel('extraction_results.xlsx', index=False)  
            logger.info("Resultados salvos em extraction_results.xlsx")
except oracledb.DatabaseError as err:
    logger.error(f"Erro ao conectar ou executar a consulta: {err}")
