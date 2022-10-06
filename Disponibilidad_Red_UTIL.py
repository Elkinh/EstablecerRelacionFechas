import pandas as pd
import numpy as np
import datetime
import calendar

pd.options.mode.chained_assignment = None  # default='warn'
nombreArchivo= 'data_dispo_red.csv'
nombreDocNodos= 'NodosR2V_TRBONET.xlsx'
directorio=  '../2. DATAS/2.2 DATAS HISTORICAS/' + nombreArchivo
directorioNodos= '../2. DATAS/2.2 DATAS HISTORICAS/' + nombreDocNodos

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
            
            #Lista para almacenar todos los clear después de que hay un CommFailure
            #variasHorasClear=[]

            #Obtener la longitud del df filtrado
            longitud=len(dfaux)

            for i in range(longitud):
                #si hay un CommFailure haga lo siguiente
                if dfaux['Severity'].iloc[i]== 'CommFailure':
                    #si ya han habido registros de fechas para guardarlas antes de modificar el indexFailure
                    #if len(variasHorasClear) >=1:        
                    #    dfaux.at[indexFailure, 'Final Falla Primer Clear'] = variasHorasClear[0] #TOMA UNICAMENTE LA PRIMERA FECHA DADAS INDICACIONES DE ADRIANO
                    #    variasHorasClear=[]
                    #    indexFailure='vacio'
                    #    horaClear='vacio'
                    #Si indexFailure está definido como "vacio"
                    indexFailure=dfaux.index[i]
                    horaClear='vacio'
                #Si hay un Clear y anteriormente ha habido un CommFailure    
                elif dfaux['Severity'].iloc[i]== 'Clear' and indexFailure!='vacio' and horaClear=='vacio':
                    horaClear=dfaux['Date/Time'].iloc[i]
                    dfaux.at[indexFailure, 'Final Falla Primer Clear'] = horaClear
                    #Guarda en el arreglo la hora en string
                    #variasHorasClear.append(horaClear.strftime('%Y-%m-%d %H:%M:%S'))
                    #variasHorasClear.append(horaClear)

                #Si es el ultimo registro y el arreglo de horas está lleno y la variable indexFailure tiene registrado un indice
                #if i== (longitud-1) and len(variasHorasClear)>=1 and indexFailure!='vacio':
                #    dfaux.at[indexFailure, 'Final Falla Primer Clear'] = variasHorasClear[0] #TOMA UNICAMENTE LA PRIMERA FECHA DADAS INDICACIONES DE ADRIANO
                #    variasHorasClear=[]
                #    indexFailure='vacio'
                #    horaClear='vacio'
            #Se pasa del df_auxiliar a la data original
            for cic in range(len(dfaux)):
                indicedata= dfaux['index'].iloc[cic]
                horaFinalFalla= dfaux['Final Falla Primer Clear'].iloc[cic]
                df.at[indicedata, 'Final Falla Primer Clear'] = horaFinalFalla
    #Elimina los Clear y los que no tienen hora final de falla
    df = data.dropna(subset=['Final Falla Primer Clear'])
    
    #Comprobación para ver todas las alertas incluso las que no tienen hora final de falla
    #df= data[data['Severity']=='CommFailure']
    
    df.reset_index(drop=True , inplace=True)
    df['Final Falla Primer Clear']= pd.to_datetime(df['Final Falla Primer Clear'])  
    return df

def indispoSegMin(df):
    df['Segundos Indispo']= (df['Final Falla Primer Clear'] - df['Date/Time']).dt.seconds
    df['Minutos Indispo']= df['Segundos Indispo']/60
    return df

def AjusteVariosDias(data):
    #Obtiene los indices donde se pasa del dia de inicio de falla 
    ind = list(np.where(data['Date/Time'].dt.day != data['Final Falla Primer Clear'].dt.day))

    for indexData in ind[0]:
        #Obtiene variables
        diaInicialAux= int(data['Date/Time'].iloc[indexData].strftime('%d'))
        diaFinalAux=int(data['Final Falla Primer Clear'].iloc[indexData].strftime('%d'))
        mes=int(data['Final Falla Primer Clear'].iloc[indexData].strftime('%m'))
        anio=int(data['Final Falla Primer Clear'].iloc[indexData].strftime('%Y'))
        
        filaIndexar= data.iloc[indexData].to_dict()
        
        for dia in range(diaInicialAux , diaFinalAux+1):
            #Este será el dia inicial
            if dia == diaInicialAux:
                #Pone la fecha final falla hasta la ultima hora del dia de la fecha del comFailure
                fechAux= pd.to_datetime( str(diaInicialAux) + str(mes) +str(anio) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S' )
                data.at[indexData, 'Final Falla Primer Clear'] = pd.to_datetime(fechAux, format='%d%m%Y %H:%M:%S' )
            elif dia != diaInicialAux and dia!= diaFinalAux:
                #Para días intermedios
                filaAux= filaIndexar.copy()
                fechIniAux= pd.to_datetime( str(dia) + str(mes) +str(anio) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S' )
                fechFinAux= pd.to_datetime( str(dia) + str(mes) +str(anio) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S' )
                filaAux['Date/Time'] = fechIniAux
                filaAux['Final Falla Primer Clear']= fechFinAux

                data.loc[len(data)]=filaAux
                
            elif dia == diaFinalAux:
                #Para el último día , o día en que la falla se terminó
                filaAux= filaIndexar.copy()
                fechIniAux= pd.to_datetime( str(diaFinalAux) + str(mes) +str(anio) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S' )
                filaAux['Date/Time'] = fechIniAux
                data.loc[len(data)]=filaAux
    return data

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

def cambiarBarranca_2(df2):
    for i in range(1,20):
        if i<=9:
            df2['Managed Resource']=df2['Managed Resource'].replace(['Barranca 2 RPT '+str(i)],'Barranc2 RPT '+str(i))
            df2['Managed Resource']=df2['Managed Resource'].replace(['Castilla '+str(i)],'Castilla RPT '+str(i))
        else:
            df2['Managed Resource']=df2['Managed Resource'].replace(['Barranca 2 RPT'+str(i)],'Barranc2 RPT '+str(i))
    return df2

def ajusteMeSiguiente(df_final):
    #Calcular si los meses de las fechas de Desconexion y Reconexion son diferentes
    df_final['MesDistinto']= np.where(((pd.DatetimeIndex(df_final['Date/Time']).month   !=  pd.DatetimeIndex(df_final['Final Falla Primer Clear']).month)),'SI','NO')
    
    ind = list(np.where(df_final['MesDistinto'] == 'SI'))
    indice= ind[0]
    
    if len(indice) > 0:
        for indexF in indice:
            #Copiar fila a modificar
            mesDuplicado=df_final.iloc[[indexF]].copy()

            #Obtener fechas Desconexion y reconexion
            fechaDesconexion=pd.to_datetime( df_final['Date/Time'].iloc[indexF] , format='%d%m%Y %H:%M:%S' )
            fechaReconexion=pd.to_datetime( df_final['Final Falla Primer Clear'].iloc[indexF] , format='%d%m%Y %H:%M:%S' )
            #Calcular Ultimo dia del mes fecha desconexion
            ultDiaMes=calendar.monthrange(fechaDesconexion.year, fechaDesconexion.month)
            
            #MODIFICAR FECHA FINAL DE FALLA PARA QUE SE TENGA EN CUENTA EN EL MES ANTERIOR
            #Seleccionar como final falla el último dia del mes
            df_final.at[indexF, 'Final Falla Primer Clear'] = pd.to_datetime( str(ultDiaMes[1]) + str(fechaDesconexion.month) +str(fechaDesconexion.year) + ' ' + '23:59:59' , format='%d%m%Y %H:%M:%S'  )
            
            #MODIFICAR FECHA INICIO DE FALLA PARA QUE SE TENGA EN CUENTA EN EL MES SIGUIENTE/ACTUAL
            mesDuplicado.at[indexF, 'Date/Time'] = pd.to_datetime( '01' + str(fechaReconexion.month) +str(fechaReconexion.year) + ' ' + '00:00:00' , format='%d%m%Y %H:%M:%S'  )

            df_final= pd.concat([df_final , mesDuplicado ])
            df_final.reset_index(inplace=True , drop=True)
    
    return df_final

def dinamicaData(df):
    df2= pd.pivot_table(df , index=['Date/Time Sin Seg','Final Falla Primer Clear Sin Seg','Nombre Nodo','AplicaIndicador','Aplica Degradacion Serv','Int Mayor a 5 Min'] ,values=['Managed Resource','Cant RPTs Nodo','Disponibilidad','Segundos Indispo'] , aggfunc={'Managed Resource':  np.count_nonzero , 'Cant RPTs Nodo': np.mean , 'Disponibilidad': np.mean , 'Segundos Indispo':np.mean})
    df2.reset_index(inplace=True)
    return df2

data= leer_archivo(directorio , 'csv')
data= cambiarBarranca_2(data)
dataNodos= leer_archivo(directorioNodos , 'xlsx')
data= formatearFecha(data)
nombresNodos= generarNombresDeNodos(data)
data= insertarNombresNodos(data , nombresNodos)
nombresNodos= GenerarConteoDeReps(nombresNodos , data)
data= generarRelacionesFechas(data, nombresNodos)
data= AjusteVariosDias(data)
data=ajusteMeSiguiente(data)
data= indispoSegMin(data)
data= Condicion_Mayor_5_Min(data)
data= Condicion_Degradacion_Servicio(data, nombresNodos)
data= relacionCantNodosTotales(data , dataNodos)
data=quitarSegundosFechas(data)
Tabla_Dinamica_data = dinamicaData(data)


'''TABLA CON NOMBRES DE NODOS , CANTIDAD DE REPETIDORES Y NOMBRES DE REPETIDORES'''
nombresNodos
'''DATA CON VALORES DE TIEMPO FINAL DE FALLA O CLEAR'''
#Tabla_Dinamica_data.to_excel('DinamicaData.xlsx')

data
Tabla_Dinamica_data
#data.to_excel("PruebaDataTRBONET_FINAL.xlsx")
