import pandas as pd
import numpy as np
import datetime

pd.options.mode.chained_assignment = None  # default='warn'
nombreArchivo= "2022-08-02_21-23-17.csv"
directorio=  "../2. DATAS/2.2 DATAS HISTORICAS/" + nombreArchivo


def leer_archivo():
    df = pd.read_csv(directorio , sep="," , encoding='latin-1')
    return df

def generarNombresDeNodos(df):
    df2= pd.DataFrame()
    df2= df['Managed Resource'].copy()
    
    #Reemplaza todas las Castilla # por solo Castilla
    df2= df2.replace(["Castilla 1", "Castilla 2", "Castilla 3" , "Castilla 4"], "Castilla")
    
    #Separa en 2 columnas para dejar solo el nombre del nodo y en otra columna el número del repetidor
    df2= df2.str.split('RPT', expand=True)
    
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
        dfaux= df[df['Nombre Nodo'].str.contains(nodos[nodo])].copy()
        
        #Obtener todos los nombres de los repetidores por nodo eliminando los duplicados
        dfaux= dfaux['Managed Resource'].drop_duplicates()
        agrupado=dfaux.tolist()
        
        NodosConReps= pd.concat([NodosConReps, pd.DataFrame([{'Nombre Nodo' : nombresNodos[nodo] , 'Cantidad de Repetidores' :len(dfaux) , 'Nombre Repetidores' : agrupado}])])
    
    NodosConReps.reset_index(drop=True , inplace=True)
    return NodosConReps

data= leer_archivo()
data= formatearFecha(data)
nombresNodos= generarNombresDeNodos(data)
data= insertarNombresNodos(data , nombresNodos)
nombresNodos= GenerarConteoDeReps(nombresNodos , data)
'''DATA'''
data

#Para realizar la prueba con el nodo castilla
auxNombre=nombresNodos.loc[0,'Nombre Nodo']
auxRep=nombresNodos.loc[0,'Nombre Repetidores']

dfPrueba= data[data['Managed Resource'].isin(auxRep)].copy()
dfPrueba.insert(8,"Final Falla", np.nan)

#'''DATAFRAME FILTRADO POR NODO CASTILLA'''
#dfPrueba

indexFailure= "vacio"
horaClear= "vacio"
for NodoRep in auxRep:
    dfaux=pd.DataFrame()
    dfaux= pd.concat([dfaux , dfPrueba[dfPrueba['Managed Resource']==NodoRep] ])
    dfaux.reset_index(inplace=True)
    
    variasHorasClear=[]
    longitud=len(dfaux)
    for i in range(longitud):
        
        if dfaux['Severity'].iloc[i]== "CommFailure":
            if len(variasHorasClear) >=1:        
                dfaux.at[indexFailure, 'Final Falla'] = variasHorasClear
                variasHorasClear=[]
                indexFailure="vacio"
                horaClear="vacio"

            indexFailure=dfaux.index[i]
            
            

        elif dfaux['Severity'].iloc[i]== "Clear" and indexFailure!="vacio":
            horaClear=dfaux['Date/Time'].iloc[i]
            variasHorasClear.append(horaClear.strftime("%Y-%m-%d %H:%M:%S"))
            
        
        if i== (longitud-1) and len(variasHorasClear)>=1 and indexFailure!="vacio":
            print(i)
            dfaux.loc[indexFailure, 'Final Falla'] = variasHorasClear
            variasHorasClear=[]
            indexFailure="vacio"
            horaClear="vacio"
    
    '''DATAFRAME FINAL PRUEBA'''
    dfaux
    for cic in range(len(dfaux)):
        indicedata= dfaux['index'].iloc[cic]
        horaFinalFalla= dfaux['Final Falla'].iloc[cic]

        data.at[indicedata, 'Final Falla'] = horaFinalFalla

'''Data con valores de tiempo final de falla'''
data.to_excel("pruebaData.xlsx")