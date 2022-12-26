import pandas as pd
import numpy as np
import datetime
import calendar
import sys

pd.options.mode.chained_assignment = None  # default='warn'
nombreArchivo= 'data_dispo_red.csv'
nombreDocNodos= 'NodosR2V_TRBONET.xlsx'
directorio=  '../2. DATAS/2.2 DATAS OPERACION/' + nombreArchivo
directorioNodos= '../2. DATAS/2.2 DATAS OPERACION/' + nombreDocNodos

def leer_archivo(nombreArchivo , tipo):
    if tipo=='csv':
        #lee el archivo en el directorio dado 
        df = pd.read_csv(nombreArchivo , sep=',' , encoding='latin-1')
        return df
    elif tipo== 'xlsx':
        #lee el archivo en el directorio dado 
        df = pd.read_excel(nombreArchivo)
        return df

def generarNombresDeNodos(df):
    df2= pd.DataFrame()
    df2= df['Managed Resource'].copy()
    
    #Separa en 2 columnas para dejar solo el nombre del nodo y en otra columna el número del repetidor
    df2= df2.str.split(' RPT', expand=True)
    
    #Para eliminar los nombres de los nodos que tienen por nombre direcciones IP que empiezan por 172
    ind = list(np.where(df2[0].str.contains('172')))
    df2.drop(ind[0] , axis=0 , inplace=True)
    
    #Elimina los nombres duplicados
    nombresNodos = df2[0].drop_duplicates()
    nombresNodos.reset_index(drop=True , inplace=True)
    return nombresNodos
    
def formatearFecha(df):
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], format='%b %d, %Y %I:%M:%S %p') - datetime.timedelta(hours= 6)
    return df

def insertarNombresNodos(df , nombresPrimarios):
    df.insert(11,'Nombre Nodo' , 'IP' )
    #Inserta el nombre del nodo al que pertenecen los repetidores
    for nombre in nombresPrimarios:
        ind = list(np.where(df['Managed Resource'].str.contains(nombre)))
        indice= ind[0]
        for j in indice:
            df['Nombre Nodo'].iloc[j] = nombre
    return df

def GenerarConteoDeReps(nodos , df):
    #Guardar en una tabla la cant de reps por nodo
    NodosConReps= pd.DataFrame(columns=['Nombre Nodo' , 'Cantidad de Repetidores','Nombre Repetidores'])
    for nodo in range(len(nodos)):
        dfaux=pd.DataFrame()
        #Filtrar por cada nombre de nodo
        dfaux= df[df['Nombre Nodo']==nodos[nodo]].copy()
        #Obtener todos los nombres de los repetidores por nodo eliminando los duplicados
        dfaux= dfaux['Managed Resource'].drop_duplicates()
        agrupado=dfaux.tolist()
        
        #Agregar a la tabla NodosConReps los valores dados
        NodosConReps= pd.concat([NodosConReps, pd.DataFrame([{'Nombre Nodo' : nombresNodos[nodo] , 'Cantidad de Repetidores' :len(dfaux) , 'Nombre Repetidores' : agrupado}])])
    
    NodosConReps.reset_index(drop=True , inplace=True)
    return NodosConReps

def generarRelacionesFechas(df, n_nodos):

    df.insert(8,'Final Falla Primer Clear', np.nan)
    #Para realizar prueba final
    for fila in range(len(nombresNodos)):
        #obtiene el nombre de todos los repetidores por nodo y los guarda en un arreglo
        auxRep=n_nodos.loc[fila,'Nombre Repetidores']
        
        #Variables que funcionan como testigos para poder realizar la operación
        indexFailure= 'vacio'
        horaClear= 'vacio'

        #Empieza a evaluar cada nombre de cada repetidor por nodo
        for NodoRep in auxRep:
            #Se genera un df auxiliar para ejecutar el filtro del repetidor en evaluación
            dfaux=pd.DataFrame()
            dfaux= pd.concat([dfaux , df[df['Managed Resource']==NodoRep] ])
            dfaux.reset_index(inplace=True)

            #Obtener la longitud del df filtrado
            longitud=len(dfaux)

            for i in range(longitud):
                #si hay un CommFailure haga lo siguiente
                if dfaux['Severity'].iloc[i]== 'CommFailure':
                    indexFailure=dfaux.index[i]
                    horaClear='vacio'

                #Si hay un Clear y anteriormente ha habido un CommFailure    
                elif dfaux['Severity'].iloc[i]== 'Clear' and indexFailure!='vacio' and horaClear=='vacio':
                    horaClear=dfaux['Date/Time'].iloc[i]
                    dfaux.at[indexFailure, 'Final Falla Primer Clear'] = horaClear

            for cic in range(len(dfaux)):
                indicedata= dfaux['index'].iloc[cic]
                horaFinalFalla= dfaux['Final Falla Primer Clear'].iloc[cic]
                df.at[indicedata, 'Final Falla Primer Clear'] = horaFinalFalla
    
    #Elimina los Clear y los que no tienen hora final de falla
    df = df.dropna(subset=['Final Falla Primer Clear'])
    df = df.dropna(subset=['Date/Time'])
    #Comprobación para ver todas las alertas incluso las que no tienen hora final de falla
    #df= data[data['Severity']=='CommFailure']
    
    df.reset_index(drop=True , inplace=True)
    df['Final Falla Primer Clear']= pd.to_datetime(df['Final Falla Primer Clear'])  
    return df

def ajustarAnios(df):
    #Evalua cuando los años son distintos
    ind = list(np.where(df['Date/Time'].dt.year != df['Final Falla Primer Clear'].dt.year))

    for ubicacion in ind[0]:
        #Copiar fila a modificar
        AnioDuplicado= df.iloc[ubicacion].to_dict()

        #Obtener fechas Inicio y Fin Estado
        fechaInicioEstado=df['Date/Time'].iloc[ubicacion]
        fechaFinEstado=df['Final Falla Primer Clear'].iloc[ubicacion]
         
        #Obtener Datos Exactos Meses
        anioInicioEstado= fechaInicioEstado.year
        diaFinEstado= fechaFinEstado.strftime("%d")
        mesFinEstado= fechaFinEstado.month
        anioFinEstado= fechaFinEstado.year
        
        for anio in range(anioInicioEstado , anioFinEstado+1):

            #Primer Año
            if anio== anioInicioEstado:
                #Para el primer registro se selecciona como final de estado el último dia del mes
                df.at[ubicacion, 'Final Falla Primer Clear'] = pd.to_datetime( '3112' + str(anio) +' ' + '23:59:59' , format='%d%m%Y %H:%M:%S'  )
            
            #Años intermedios
            elif (anio != anioInicioEstado) and (anio != anioFinEstado):
                AnioDuplicado['Date/Time'] = pd.to_datetime( '0101' + str(anio) +' ' + '00:00:00' , format='%d%m%Y %H:%M:%S'  )
                AnioDuplicado['Final Falla Primer Clear'] = pd.to_datetime( '3112' + str(anio) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S'  )
                df.loc[len(df)]=AnioDuplicado
            #Año Final
            elif anio == anioFinEstado:
                AnioDuplicado['Date/Time'] = pd.to_datetime( '0101' +str(anio) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S'  )
                AnioDuplicado['Final Falla Primer Clear'] = pd.to_datetime( str(diaFinEstado) + str(mesFinEstado) +str(anio) + ' ' + fechaFinEstado.strftime('%H:%M:%S') , format='%d%m%Y %H:%M:%S'  )
                df.loc[len(df)]=AnioDuplicado
    return df

def ajustarMeses(df):
    #Evalua cuando los meses son distintos
    ind = list(np.where(df['Date/Time'].dt.month != df['Final Falla Primer Clear'].dt.month))

    for ubicacion in ind[0]:
        #Copiar fila a modificar
        mesDuplicado= df.iloc[ubicacion].to_dict()

        #Obtener fechas Inicio y Fin Estado
        fechaInicioEstado=df['Date/Time'].iloc[ubicacion]
        fechaFinEstado=df['Final Falla Primer Clear'].iloc[ubicacion]
        
        #Obtener Datos Exactos Meses
        mesInicioEstado= fechaInicioEstado.month
        anioInicioEstado= fechaInicioEstado.year
        diaFinEstado= fechaFinEstado.strftime("%d")
        mesFinEstado= fechaFinEstado.month
        anioFinEstado= fechaFinEstado.year


        if anioInicioEstado == anioFinEstado:
        
            for mes in range(mesInicioEstado , mesFinEstado+1):
                #Calcular Ultimo dia del mes fecha Inicio de Estado
                ultDiaMes=calendar.monthrange(fechaInicioEstado.year, mes)
                auxmes= str(mes)
                if len(auxmes) != 2:
                    auxmes= '0' + auxmes
                #Primer Mes
                if mes== mesInicioEstado:
                    #Para el primer registro se selecciona como final de estado el último dia del mes
                    df.at[ubicacion, 'Final Falla Primer Clear'] = pd.to_datetime( str(ultDiaMes[1]) + str(auxmes) +str(anioInicioEstado) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S'  )
                
                #Meses intermedios
                elif (mes != mesInicioEstado) and (mes != mesFinEstado):
                    mesDuplicado['Date/Time'] = pd.to_datetime( '01' + str(auxmes) +str(anioInicioEstado) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S'  )
                    mesDuplicado['Final Falla Primer Clear'] = pd.to_datetime( str(ultDiaMes[1]) + str(auxmes) +str(anioInicioEstado) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S'  )
                    df.loc[len(df)]=mesDuplicado
                #Mes Final
                elif mes == mesFinEstado:
                    mesDuplicado['Date/Time'] = pd.to_datetime( '01' + str(auxmes) +str(anioInicioEstado) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S'  )
                    mesDuplicado['Final Falla Primer Clear'] = pd.to_datetime( str(diaFinEstado) + str(auxmes) +str(anioInicioEstado) + ' ' + fechaFinEstado.strftime('%H:%M:%S') , format='%d%m%Y %H:%M:%S'  )
                    df.loc[len(df)]=mesDuplicado

        else:
            print("Años distintos, ajustar")

    return df

def ajustarDias(df):
    df= df.sort_values(by='Date/Time')
    df.reset_index(inplace=True , drop=True)

    ind = list(np.where( df['Date/Time'].dt.day != df['Final Falla Primer Clear'].dt.day) )
    
    for indexData in ind[0]:
        #Copiar Fila a modificar
        filaIndexar= df.iloc[indexData].to_dict()

        #Obtener Fecha de Fin Estado
        fechaFinEstado2=df['Final Falla Primer Clear'].iloc[indexData]
        
        #Obtiene variables
        diaInicialAux= int(df['Date/Time'].iloc[indexData].strftime('%d'))
        
        if not pd.isnull(df['Final Falla Primer Clear'].iloc[indexData]):
            diaFinalAux=fechaFinEstado2.day
            mes=int(df['Final Falla Primer Clear'].iloc[indexData].strftime('%m'))
            anio=int(df['Final Falla Primer Clear'].iloc[indexData].strftime('%Y'))
        
        #Si no está vacio el campo de cambio de estado
        if diaFinalAux:

            for dia in range(diaInicialAux , diaFinalAux+1):
                auxdia= str(dia)
                if len(auxdia) != 2:
                    auxdia= '0' + auxdia

                #Este será el dia inicial
                if dia == diaInicialAux:
                    #Pone la fecha final del estado del caso hasta la ultima hora del dia
                    fechAux= pd.to_datetime( str(auxdia) + str(mes) +str(anio) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S' , errors='coerce' )
                    df.at[indexData, 'Final Falla Primer Clear'] = pd.to_datetime(fechAux, format='%d%m%Y %H:%M:%S' , errors='coerce')
                
                elif dia != diaInicialAux and dia!= diaFinalAux:
                    #Para días intermedios
                    filaAux= filaIndexar.copy()
                    fechIniAux= pd.to_datetime( str(auxdia) + str(mes) +str(anio) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S', errors='coerce' )
                    fechFinAux= pd.to_datetime( str(auxdia) + str(mes) +str(anio) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S', errors='coerce' )
                    filaAux['Date/Time'] = fechIniAux
                    filaAux['Final Falla Primer Clear']= fechFinAux
                    df.loc[len(df)]=filaAux
                    
                elif dia == diaFinalAux:
                    #Para el último día , o día en que la falla finalizó
                    filaAux= filaIndexar.copy()
                    fechIniAux= pd.to_datetime( str(auxdia) + str(mes) +str(anio) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S', errors='coerce' )
                    filaAux['Date/Time'] = fechIniAux
                    df.loc[len(df)]=filaAux

    return df

def indispoSegMin(df):
    df['Segundos Indispo']= (df['Final Falla Primer Clear'] - df['Date/Time']).dt.seconds
    df['Minutos Indispo']= df['Segundos Indispo']/60
    return df

def Condicion_Mayor_5_Min(df):
    df["Int Mayor a 5 Min"] = "NO" 
    ind = list(np.where(df['Segundos Indispo'] >= 300))
    indice= ind[0]
    for j in indice:
        df['Int Mayor a 5 Min'].iloc[j] = "SI"
    return df

def Condicion_Degradacion_Servicio(df , nodosReps):
    for fila in range(len(nodosReps)):
        for repetidor in nodosReps['Nombre Repetidores'].iloc[fila]:
            #Se genera un df auxiliar para filtrar por el repetidor en evaluación
            dfaux=pd.DataFrame()
            dfaux= pd.concat([dfaux , df[df['Managed Resource']==repetidor] ])
            dfaux.reset_index(inplace=True)
            dfaux= dfaux.sort_values(by='Date/Time') 
            #Obtener la longitud del df filtrado
            longitud=len(dfaux)
            
            dfaux['Lapso Entre Fallas']=0
            dfaux['Aplica Degradacion Serv']="NO"
            for i in range( 1 , longitud ):
                
                final= dfaux['Final Falla Primer Clear'].iloc[i-1]
                inicial= dfaux['Date/Time'].iloc[i]
                
                dif=(inicial - final).total_seconds()
                dfaux['Lapso Entre Fallas'].iloc[i]= dif

                if dif <= 3600:
                    dfaux['Aplica Degradacion Serv'].iloc[i-1]='SI'
                    dfaux['Aplica Degradacion Serv'].iloc[i]='SI'

            #Se pasa del df_auxiliar a la data original
            for cic in range(len(dfaux)):
                indicedata= dfaux['index'].iloc[cic]
                lapso= dfaux['Lapso Entre Fallas'].iloc[cic]
                aplica= dfaux['Aplica Degradacion Serv'].iloc[cic]
                df.at[indicedata, 'Lapso Entre Fallas'] = lapso
                df.at[indicedata, 'Aplica Degradacion Serv'] = aplica        
    return df

def relacionCantNodosTotales(df_final, dataOficialNodos):
    df_final['Cant RPTs Nodo']= np.nan
    for i in range(len(dataOficialNodos)):
        ind = list(np.where(df_final['Nombre Nodo'] == dataOficialNodos['Nombre Nodo'].iloc[i]))
        indice= ind[0]
        for j in indice:
            df_final['Cant RPTs Nodo'].iloc[j] = int(dataOficialNodos['Cantidad de Repetidores'].iloc[i])
    return df_final

def quitarSegundosFechas(data):
    data["Disponibilidad"]= 1 - (data['Segundos Indispo'] / 86400)
    data['Date/Time Sin Seg'] = data['Date/Time'].dt.strftime('%d/%m/%Y %H:%M')
    data['Final Falla Primer Clear Sin Seg'] = data['Final Falla Primer Clear'].dt.strftime('%d/%m/%Y %H:%M')
    data['AplicaIndicador']= np.where(((data['Aplica Degradacion Serv']=='SI') | (data['Int Mayor a 5 Min']=='SI')),'SI','NO')
    return data

def AjustarNombres(df2):
    for i in range(1,20):
        if i<=9:
            df2['Managed Resource']=df2['Managed Resource'].replace(['Barranca 2 RPT '+str(i)],'Barranc2 RPT '+str(i))
            df2['Managed Resource']=df2['Managed Resource'].replace(['Provin RPT '+str(i)+ ' Mot'],'Provin2 RPT '+str(i)+ ' Mot')
            df2['Managed Resource']=df2['Managed Resource'].replace(['Castilla '+str(i)],'Castilla RPT '+str(i))
            
        else:
            df2['Managed Resource']=df2['Managed Resource'].replace(['Barranca 2 RPT'+str(i)],'Barranc2 RPT '+str(i))
    return df2

def dinamicaData(df):
    df2= pd.pivot_table(df , index=['Date/Time Sin Seg','Final Falla Primer Clear Sin Seg','Nombre Nodo','AplicaIndicador','Aplica Degradacion Serv','Int Mayor a 5 Min'] ,values=['Event ID','Managed Resource','Cant RPTs Nodo','Disponibilidad','Segundos Indispo'] , aggfunc={'Event ID': 'min' , 'Managed Resource':  np.count_nonzero , 'Cant RPTs Nodo': np.mean , 'Disponibilidad': np.mean , 'Segundos Indispo':np.mean })
    df2.reset_index(inplace=True)
    return df2

def cruzarPowerBI(df_limpio):
    df= df_limpio.copy()
    #Formatea a fecha
    df['Date/Time Sin Seg']=pd.to_datetime(df['Date/Time Sin Seg'] , format= '%d/%m/%Y %H:%M')
    df['Final Falla Primer Clear Sin Seg']=pd.to_datetime(df['Final Falla Primer Clear Sin Seg'], format= '%d/%m/%Y %H:%M')
    #Calcula los minutos
    df['Minutos Indispo']= df['Segundos Indispo'] / 60
    #Valida si todos los nodos fallaron con respecto al total
    df['AplicaFalla']= np.where((df['Managed Resource']==df['Cant RPTs Nodo']),'SI','NO')
    #Filtros
    df= df[(df['AplicaIndicador']=='SI') & (df['AplicaFalla']=='SI')]
    #Nodos Que No Aplican Todavía
    df= df[~df['Nombre Nodo'].isin(['Churuyaco', 'ISEC','Provin','Provincia','Sucumbios'])]
    df.reset_index(drop=True , inplace=True)
    #Filtra por el mes actual
    mes = datetime.datetime.today().month
    df = df[df['Date/Time Sin Seg'].dt.month == mes]
    df['Dia']=df['Date/Time Sin Seg'].dt.day
    df= df.sort_values(by='Date/Time Sin Seg')
    #Renombra los nombres de las columnas para que sean iguales al Power BI
    df = df.rename(columns={'Date/Time Sin Seg':'Fecha Desconexion',
                            'Final Falla Primer Clear Sin Seg':'Fecha Reconexion',
                            'Nombre Nodo':'Nodo',
                            'Minutos Indispo':'Tiempo De Afectacion (Min)'})
    #Exporta la tabla con las columnas especificas
    df_export= df[['Fecha Desconexion', 'Fecha Reconexion','Nodo','Tiempo De Afectacion (Min)','Dia','Event ID','Aplica Degradacion Serv']].copy()
    df_export.to_excel('C:/Users/ehernandez04/OneDrive - ITS InfoCom/ANALITICA ITS/POWER BI/CLIENTES/COLOMBIA/UTIL/2. DATAS/2.2 DATAS OPERACION/DisponibilidadDinamico.xlsx' , index=False)
    #df_export.to_excel('E:/ONE DRIVE ANALITICA/OneDrive - ITS InfoCom/ANALITICA ITS/POWER BI/CLIENTES/COLOMBIA/UTIL/2. DATAS/2.2 DATAS OPERACION/DisponibilidadDinamico.xlsx' , index=False)


#Lectura De Archivos
dataNodos= leer_archivo(directorioNodos , 'xlsx')    
data= leer_archivo(directorio , 'csv')

data= AjustarNombres(data) #Ajustar los nombres que sean necesarios
data= formatearFecha(data) #Le pone formato de fecha a la columna Date/Time

#Filtra columna para que muestre eventos de falla , no de comunicacion ni de otro tipo
data=data[data['Entity']=='NcmSynchronization']
data.reset_index(drop=True , inplace=True)

nombresNodos= generarNombresDeNodos(data) #Genera los nombres de los nodos en un DF a parte
data= insertarNombresNodos(data , nombresNodos) #Inserta los nombres de los nodos en el df principal (Data)
nombresNodos= GenerarConteoDeReps(nombresNodos , data) #Cuenta el numero de repetidores registradas en el df principal (Data)
data= generarRelacionesFechas(data, nombresNodos) #Se realiza el procesamiento de los commFailure y los clear

#Ajuste entre lapsos de tiempo para segmentar las fechas de las fallas por días
data= ajustarAnios(data)
data=ajustarMeses(data)
data= ajustarDias(data)


data= indispoSegMin(data) #Calcula el tiempo de indisponibilidad en segundos y en minutos
data= Condicion_Mayor_5_Min(data) #Agrega Columna para semaforizar si la condicion es mayor a 5 Min
data= Condicion_Degradacion_Servicio(data, nombresNodos) #Agrega Columna para semaforizar si aplica degradacion de servicio
data= relacionCantNodosTotales(data , dataNodos) #Agrega al df principal (Data) la cantidad de reps por nodo que se registran en el excel NodosR2v
data=quitarSegundosFechas(data) #Se eliminan segundos de las fechas para poder realizar la tabla dinamica
Tabla_Dinamica_data = dinamicaData(data) #Se realiza la tabla dinámica de los datos

#paraExportar= cruzarPowerBI(Tabla_Dinamica_data)
'''TABLA CON NOMBRES DE NODOS , CANTIDAD DE REPETIDORES Y NOMBRES DE REPETIDORES'''
#nombresNodos
'''DATA CON VALORES DE TIEMPO FINAL DE FALLA O CLEAR'''
#data
#Tabla_Dinamica_data
Tabla_Dinamica_data.to_excel("../2. DATAS/2.2 DATAS OPERACION/Fuente Datos Dispo Red Diario.xlsx" ,index= False )
nombresNodos.to_excel("../2. DATAS/2.2 DATAS OPERACION/Nodos Procesados Python.xlsx" ,index= False)
