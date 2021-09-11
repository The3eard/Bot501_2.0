# bot.py
import asyncio
import csv
import datetime
import logging
import os
import re
import sqlite3
from sqlite3 import Error

import discord
import numpy
import pandas as pd
import requests
from discord.ext import commands
from dotenv import load_dotenv
from steam.webapi import WebAPI

logging.basicConfig(level=logging.INFO)

load_dotenv()

config = {
    'token': os.environ['DISCORD_TOKEN'],
    'steam_api_key': os.environ["STEAM_API_KEY"]
}

database_file = r"div501.db"
bot = commands.Bot(command_prefix='-')


def sql_connection():
    try:
        con = sqlite3.connect(database_file)
        # print("Connection is established: Database is created in memory")
        return (con)
    except Error:
        print(Error)


def sql_fetch(query):
    try:
        # print(query)
        con = sql_connection()
        cursor = con.cursor()
        cursor.execute(query)
        query = cursor.fetchall()
        # print("Record fetch successfully")
        return (query)
    except sqlite3.Error as error:
        print("Failed to fetch sqlite table", error)
    finally:
        if (con):
            con.close()
            # print("The SQLite connection is closed")


def sql_update(query):
    try:
        # print(query)
        con = sql_connection()
        cursor = con.cursor()
        cursor.execute(query)
        con.commit()
        # print("Record Updated successfully")
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
    finally:
        if (con):
            con.close()
            # print("The SQLite connection is closed")


def update_steam_hll(steamid64, campo):
    # Obtenemos horas jugadas en steam
    try:
        steamId = str(steamid64)
        appId = "686810"
        api = WebAPI(key=config['steam_api_key'])
        hll_horas = "Perfil de STEAM privado"
        query = 'UPDATE Miembros SET ' + campo + ' = "' + str(hll_horas) + '" WHERE SteamID64 = ' + steamId
        sql_update(query)
        response = api.IPlayerService.GetOwnedGames(steamid=steamId, include_appinfo=False,
                                                    include_played_free_games=False, include_free_sub=False,
                                                    appids_filter=[appId])
        hll_minutos = response['response']['games'][0]['playtime_forever']
        hll_horas = int(hll_minutos / 60)
        query = 'UPDATE Miembros SET ' + campo + ' = "' + str(hll_horas) + '" WHERE SteamID64 = ' + steamId
        sql_update(query)
    except:
        hll_horas = "Perfil de STEAM privado"
    return (hll_horas)


def obtener_clase(idclase):
    # Cambiamos la ID por el nombre de la clase
    query = 'SELECT *FROM "Clases" where idClase=' + str(idclase)
    columnas_clases = ('idClase', 'Nombre')
    datos_clases = pd.DataFrame(sql_fetch(query), columns=columnas_clases)
    clase = datos_clases['Nombre'].values[0]
    return (clase)


def obtener_amonestaciones(member):
    # Sumamos las amonestaciones
    query = 'select * from  Amonestaciones where idMiembro=' + str(member.id)
    columnas_amonestaciones = ('IdAmonestacion', 'IdMiembro', 'Amonestacion')
    amonestaciones = pd.DataFrame(sql_fetch(query), columns=columnas_amonestaciones)
    n_amonestaciones = amonestaciones.shape[0]
    return (n_amonestaciones)


def change_date_format(dt):
    return re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\3-\\2-\\1', dt)


def obtener_estadisticas(member):
    # Obtenemos las estadisticas de la BD
    query = 'SELECT TipoPartida,Fecha,Muertes,Bajas,idClase,Funcion,Curados,VehiculosDestruidos,EstructurasConstruidas,AusenciaUltimoMomento,Ausencia,Desapuntado FROM "Partidas" INNER JOIN "Estadisticas" ON Estadisticas.idPartida = Partidas.idPartida WHERE idMiembro=' + str(
        member.id) + ' ORDER BY date(Fecha) ASC'
    columnas_estadisticas = (
        'TipoPartida', 'Fecha', 'Muertes', 'Bajas', 'idClase', 'Funcion', 'Curados', 'VehiculosDestruidos',
        'EstructurasConstruidas', 'AusenciaUltimoMomento', 'Ausencia', 'Desapuntado')
    datos_estadisticas = pd.DataFrame(sql_fetch(query), columns=columnas_estadisticas)
    # Comprobamos si hay partidas jugadas
    if datos_estadisticas[
        (datos_estadisticas['TipoPartida'] == 'Oficial') & (datos_estadisticas['AusenciaUltimoMomento'] == 0) & (
                datos_estadisticas['Ausencia'] == 0) & (datos_estadisticas['Desapuntado'] == 0)].shape[0] == 0:
        fecha_upartida = "-"
    else:
        fecha_upartida = datos_estadisticas[
            (datos_estadisticas['TipoPartida'] == 'Oficial') & (datos_estadisticas['AusenciaUltimoMomento'] == 0) & (
                    datos_estadisticas['Ausencia'] == 0) & (datos_estadisticas['Desapuntado'] == 0)]['Fecha'].iloc[
            -1]
    # Comprobamos si hay entrenamientos jugados
    if datos_estadisticas[
        (datos_estadisticas['TipoPartida'] == 'Entrenamiento') & (datos_estadisticas['AusenciaUltimoMomento'] == 0) & (
                datos_estadisticas['Ausencia'] == 0) & (datos_estadisticas['Desapuntado'] == 0)].shape[0] == 0:
        fecha_uentrenamiento = "-"
    else:
        fecha_uentrenamiento = datos_estadisticas[(datos_estadisticas['TipoPartida'] == 'Entrenamiento') & (
                datos_estadisticas['AusenciaUltimoMomento'] == 0) & (datos_estadisticas['Ausencia'] == 0) & (
                                                          datos_estadisticas['Desapuntado'] == 0)]['Fecha'].iloc[-1]
    # Empezamos a obtener valores del dataframe
    Muertes = int(datos_estadisticas['Muertes'].sum())
    Bajas = int(datos_estadisticas['Bajas'].sum())
    # Si las variables son 0 no calculamos
    if Muertes == 0 and Bajas == 0:
        Muertes = "0"
        Bajas = "0"
        KD = "0"
    else:
        KD = Muertes / Bajas
    # Seguimos obteniendo valores del dataframe
    Curados = int(datos_estadisticas['Curados'].sum())
    VehiculosDestruidos = int(datos_estadisticas['VehiculosDestruidos'].sum())
    EstructurasConstruidas = int(datos_estadisticas['EstructurasConstruidas'].sum())
    # Si no hay valores lo dejamos en -
    if datos_estadisticas.shape[0] == 0:
        ClaseMasUsada = "-"
        FuncionMasJugada = "-"
    else:
        ClaseMasUsada = obtener_clase(datos_estadisticas['idClase'].mode().values[0])
        FuncionMasJugada = datos_estadisticas['Funcion'].mode().values[0]
    # Seguimos obteniendo valores del dataframe
    partidas_apuntado = (datos_estadisticas['TipoPartida'] == 'Oficial').sum()
    partidas_ausente_um = ((datos_estadisticas['TipoPartida'] == 'Oficial') & (
            datos_estadisticas['AusenciaUltimoMomento'] == 1)).sum()
    partidas_ausente = ((datos_estadisticas['TipoPartida'] == 'Oficial') & (datos_estadisticas['Ausencia'] == 1)).sum()
    partidas_desapuntado = (
            (datos_estadisticas['TipoPartida'] == 'Oficial') & (datos_estadisticas['Desapuntado'] == 1)).sum()
    partidas_jugadas = partidas_apuntado - partidas_ausente_um - partidas_ausente - partidas_desapuntado
    entrenamientos_apuntado = (datos_estadisticas['TipoPartida'] == 'Entrenamiento').sum()
    entrenamientos_ausente_um = ((datos_estadisticas['TipoPartida'] == 'Entrenamiento') & (
            datos_estadisticas['AusenciaUltimoMomento'] == 1)).sum()
    entrenamientos_ausente = (
            (datos_estadisticas['TipoPartida'] == 'Entrenamiento') & (datos_estadisticas['Ausencia'] == 1)).sum()
    entrenamientos_desapuntado = (
            (datos_estadisticas['TipoPartida'] == 'Entrenamiento') & (datos_estadisticas['Desapuntado'] == 1)).sum()
    entrenamientos_jugados = entrenamientos_apuntado - entrenamientos_ausente_um - entrenamientos_ausente - entrenamientos_desapuntado
    return (
        fecha_upartida, fecha_uentrenamiento, KD, Muertes, Bajas, Curados, VehiculosDestruidos, EstructurasConstruidas,
        ClaseMasUsada, FuncionMasJugada, partidas_apuntado, partidas_jugadas, partidas_ausente, partidas_ausente_um,
        partidas_desapuntado, entrenamientos_apuntado, entrenamientos_jugados, entrenamientos_ausente,
        entrenamientos_ausente_um, entrenamientos_desapuntado)


def escribir_ficha(member):
    # Variables y funciones
    avatar = member.avatar_url
    nick = member.display_name
    estadisticas = obtener_estadisticas(member)
    n_amonestaciones = obtener_amonestaciones(member)

    # Querys de consulta de la BD
    query = 'SELECT *FROM "Miembros" where idMiembro=' + str(member.id)
    columnas_ficha = (
        'idMiembro', 'Nick', 'Nombre', 'Localidad', 'FechaNacimiento', 'Clase', 'FechaIngreso', 'DisponibilidadHoraria',
        'UltimaVezVoz', 'UltimaVezChat', 'SteamID64', 'SteamHLL', 'SteamHLL_Inicio', 'Streaming', 'Observaciones')
    datos_ficha = pd.DataFrame(sql_fetch(query), columns=columnas_ficha)

    # Obtener campos de las querys
    get_Steam = str(datos_ficha['SteamID64'].values[0])
    SteamHLL = str(update_steam_hll(get_Steam, "SteamHLL"))
    get_Nick = str(datos_ficha['Nick'].values[0])
    get_Nombre = str(datos_ficha['Nombre'].values[0])
    get_Localidad = str(datos_ficha['Localidad'].values[0])
    get_FechaNacimiento = str(datos_ficha['FechaNacimiento'].values[0])
    get_Clase = obtener_clase(str(datos_ficha['Clase'].values[0]))
    get_FechaIngreso = str(datos_ficha['FechaIngreso'].values[0])
    get_Disponibilidad = str(datos_ficha['DisponibilidadHoraria'].values[0])
    get_UltimaVezVoz = str(datos_ficha['UltimaVezVoz'].values[0])
    get_UltimaVezChat = str(datos_ficha['UltimaVezChat'].values[0])
    get_SteamHLL_Inicio = str(datos_ficha['SteamHLL_Inicio'].values[0])
    get_Streaming = str(datos_ficha['Streaming'].values[0])
    get_Observaciones = str(datos_ficha['Observaciones'].values[0])

    # Creacion del cuerpo EMBED de la ficha
    embed = discord.Embed(
        title="Datos",
        description="Nick: " + get_Nick +
                    " - Clase preferida: " + get_Clase +
                    "\nNombre: " + get_Nombre +
                    " - Localidad: " + get_Localidad +
                    "\nFecha de nacimiento: " + str(change_date_format(get_FechaNacimiento))[0:10] +
                    "\nFecha de ingreso: " + str(change_date_format(get_FechaIngreso)),
        color=0x109319
    )
    embed.set_author(name=nick, url="https://steamcommunity.com/profiles/" + get_Steam, icon_url=avatar)
    embed.set_thumbnail(url=avatar)
    embed.add_field(
        name="Actividad",
        value="Disponibilidad: " + get_Disponibilidad +
              "\nUltima vez en canal de voz: " + str(change_date_format(get_UltimaVezVoz)) +
              "\nUltimo mensaje: " + str(change_date_format(get_UltimaVezChat)) +
              "\nUltimo evento: " + str(change_date_format(estadisticas[0])) +
              "\nUltimo entrenamiento: " + str(change_date_format(estadisticas[1])) +
              "\nHoras steam HLL inicial: " + get_SteamHLL_Inicio +
              "\nHoras steam HLL: " + SteamHLL,
        inline=False
    )
    embed.add_field(
        name="Estadisticas",
        value="KD: " + str(estadisticas[2])[0:4] +
              "\nMuertes: " + str(estadisticas[3]) +
              "\nBajas: " + str(estadisticas[4]) +
              "\nCurados: " + str(estadisticas[5]) +
              "\nVehiculos Destruidos: " + str(estadisticas[6]) +
              "\nEstructuras Construidas: " + str(estadisticas[7]) +
              "\nClase mas jugada: " + str(estadisticas[8]) +
              "\nFunción más jugada: " + str(estadisticas[9]) +
              "\nPartidas jugadas: " + str(estadisticas[10]) +
              "\nEntrenamientos: " + str(estadisticas[16]),
        inline=False
    )
    embed.add_field(
        name="Gestión",
        value="Partidas apuntado: " + str(estadisticas[10]) +
              "\nPartidas jugadas: " + str(estadisticas[11]) +
              "\nAusencias partidas: " + str(estadisticas[12]) +
              "\nAusencias ultima hora partidas: " + str(estadisticas[13]) +
              "\nDesapuntado de partidas: " + str(estadisticas[14]) +
              "\nEntrenamientos apuntado: " + str(estadisticas[15]) +
              "\nEntrenamientos: " + str(estadisticas[16]) +
              "\nAusencias entrenamientos: " + str(estadisticas[17]) +
              "\nAusencias ultima hora entrenamientos: " + str(estadisticas[18]) +
              "\nDesapuntado de entrenamientos: " + str(estadisticas[19]) +
              "\nAmonestaciones: " + str(n_amonestaciones) +
              "\nStreaming: " + str(get_Streaming) +
              "\nObservaciones: " + get_Observaciones,
        inline=False
    )
    embed.set_footer(text="This is the footer. It contains text at the bottom of the embed")
    return (embed)


def escribir_nueva_ficha(campos, datos, titulo):
    datos_campos = ""
    for i in range((len(campos) - 1)):
        datos_campos = str(datos_campos) + str(campos[i]) + ": " + str(datos[i]) + "\n"
    embed = discord.Embed(
        title=titulo + " ficha",
        description=datos_campos,
        color=0x109319)
    embed.set_footer(
        text="Clases:\n1 - Comandante, 2 - Oficial, 3 - Fusilero, 4 - Asalto, 5 - Fusilero automatico, \n6 - Medico, 7 - Apoyo, 8 - Antitanque, 9 - Ametralladora, 10 - Ingeniero, \n11 - Oficial tanque, 12 - Tripulante, 13 - Oficial recon, 14 - Francotirador")
    return (embed)


def consultar_miembro(id):
    query = 'SELECT idMiembro,Nick,Nombre,Localidad,FechaNacimiento,Clase,DisponibilidadHoraria,SteamID64,SteamHLL_Inicio,Streaming from Miembros where idMiembro=' + str(
        id)
    datos_miembro = sql_fetch(query)
    check = datos_miembro != []
    return (check, datos_miembro)


@bot.event
async def on_ready():
    print('We have logged in as {0.user}'.format(bot))


@bot.event
async def on_message(message):
    # Si el mensaje es del bot haz NADA
    if message.author == bot.user:
        return
    # Cogemos hora de cualquier mensaje y lo registramos
    from datetime import datetime
    date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    query = 'UPDATE Miembros SET UltimaVezChat = "' + str(date) + '" WHERE idMiembro = ' + str(message.author.id)
    sql_update(query)
    await bot.process_commands(message)


@bot.event
async def on_voice_state_update(member, before, after):
    # Cada vez que se desconecte alguien lo registramos
    from datetime import datetime
    date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    if "None" in str(after):
        query = 'UPDATE Miembros SET UltimaVezVoz = "' + str(date) + '" WHERE idMiembro = ' + str(member.id)
        sql_update(query)


@bot.command(pass_context=True)
async def ficha(ctx, arg1, member: discord.Member):
    # Comando para ver ficha
    if arg1 == "ver":
        try:
            embed = escribir_ficha(member.id)
            await ctx.channel.send(embed=embed)
        except Error:
            await ctx.channel.send(Error)

    # Comando para crear ficha
    if arg1 == "crear":
        try:
            # Si el miembro ya existe paramos.
            if consultar_miembro(member.id)[0] == True:
                await ctx.send("La ficha del miembro ya existe.")
                return
            # Escribimos la ficha de ejemplo
            FechaIngreso = member.joined_at.strftime('%Y-%m-%d %H:%M:%S')
            campos = ['Nick', 'Nombre', 'Localidad', 'Fecha de Nacimiento', 'Clase Preferida', 'Disponibilidad',
                      'SteamID64', 'Streaming (Si/No)', 'Confirmar o repetir']
            datos = ['LetalDark', 'Albert', 'Barcelona', '19-08-1988', '1', 'Entre semana por la tarde',
                     '76561198003499120', 'Si', '-']
            await ctx.send("**Responde a las preguntas.**\nSi quieres volver a escribir un campo pon ``repetir``\n\n")
            nueva_ficha = await ctx.send(embed=escribir_nueva_ficha(campos, datos, "Crear"))
            global index_c
            index_c = 0
            while index_c < len(campos):
                pregunta = await ctx.send(str(campos[index_c]) + ":")

                # Comprobamos que los mensajes tienen los valores requeridos
                def check_ficha(respuesta):
                    # Comprobamos si los mensajes estan en su canal y son del mismo autor
                    if respuesta.author.id != ctx.author.id or respuesta.channel.id != ctx.channel.id:
                        check = False
                    else:
                        if str(campos[index_c]) in ("Nick", "Nombre", "Localidad", "Disponibilidad"):
                            check = len(respuesta.content) >= 1
                        if str(campos[index_c]) in ("Fecha de Nacimiento"):
                            check = len(respuesta.content) == 10
                        if str(campos[index_c]) in ("Clase Preferida"):
                            try:
                                check = int(respuesta.content) > 0 and int(respuesta.content) <= 14
                            except:
                                check = False
                        if str(campos[index_c]) in ("SteamID64"):
                            check = len(respuesta.content) == 17
                        if str(campos[index_c]) in ("Streaming (Si/No):"):
                            check = respuesta.content == "Si" or respuesta.content == "No"
                        if str(campos[index_c]) == "Confirmar o repetir":
                            check = respuesta.content == "confirmar" or respuesta.content == "Confirmar"
                        if respuesta.content == "repetir":
                            check = True
                    return (check)

                respuesta = await bot.wait_for("message", check=(check_ficha), timeout=60)
                # Borramos mensajes de preguntas y respuestas
                await pregunta.delete()
                await respuesta.delete()
                # Ordenamos los mensajes, si tiene repetir volvemos atras
                if respuesta.content == "repetir":
                    if index_c == 0:
                        index_c = 0
                    else:
                        index_c = index_c - 1
                else:
                    datos[index_c] = respuesta.content
                    await nueva_ficha.edit(embed=escribir_nueva_ficha(campos, datos, "Nueva"))
                    index_c += 1
            datetime_nacimiento = datetime.datetime.strptime(str(datos[3]) + " 00:00:00", '%d-%m-%Y %H:%M:%S')
            # Introducimos los datos en la BD
            query = 'INSERT INTO Miembros (idMiembro,Nick,Nombre,Localidad,FechaNacimiento,Clase,FechaIngreso,DisponibilidadHoraria,SteamID64,Streaming) VALUES(' + str(
                member.id) + ',"' + str(datos[0]) + '","' + str(datos[1]) + '","' + str(datos[2]) + '","' + str(
                datetime_nacimiento) + '",' + str(datos[4]) + ',"' + str(FechaIngreso) + '","' + str(
                datos[5]) + '",' + str(datos[6]) + ',"' + str(datos[7]) + '")'
            sql_update(query)
            update_steam_hll(datos[6], "SteamHLL_Inicio")
            await ctx.send("Ficha de " + str(member) + " creada.")
        except asyncio.TimeoutError:
            await ctx.send("Tiempo de espera agotado (60s).")
        except Error:
            await ctx.channel.send(Error)

    # Comando para ver ficha
    if arg1 == "editar":
        try:
            # Si el miembro no existe paramos.
            datos_miembro = consultar_miembro(member)
            if datos_miembro[0] == False:
                await ctx.send("La ficha del miembro no existe.")
                return
            # Obtener campos de las querys
            columnas_ficha = (
                'idMiembro', 'Nick', 'Nombre', 'Localidad', 'FechaNacimiento', 'Clase', 'DisponibilidadHoraria',
                'SteamID64', 'SteamHLL_Inicio', 'Streaming')
            datos_ficha = pd.DataFrame(datos_miembro[1], columns=columnas_ficha)
            get_Nick = str(datos_ficha['Nick'].values[0])
            get_Nombre = str(datos_ficha['Nombre'].values[0])
            get_Localidad = str(datos_ficha['Localidad'].values[0])
            try:
                get_FechaNacimiento = str(change_date_format(datos_ficha['FechaNacimiento'].values[0])[0:10])
            except:
                get_FechaNacimiento = "-"
            get_Clase = obtener_clase(str(datos_ficha['Clase'].values[0]))
            get_Disponibilidad = str(datos_ficha['DisponibilidadHoraria'].values[0])
            get_Steam = str(datos_ficha['SteamID64'].values[0])
            get_SteamHLL_Inicio = str(datos_ficha['SteamHLL_Inicio'].values[0])
            get_Streaming = str(datos_ficha['Streaming'].values[0])
            # Escribimos la ficha actual
            campos = ['1 - Nick', '2 - Nombre', '3 - Localidad', '4 - Fecha de Nacimiento', '5 - Clase Preferida',
                      '6 - Disponibilidad', '7 - SteamID64', '8 - Streaming (Si/No)', 'Confirmar o repetir']
            datos = [get_Nick, get_Nombre, get_Localidad, get_FechaNacimiento, get_Clase, get_Disponibilidad, get_Steam,
                     get_Streaming, '-']
            await ctx.send("**Elige un campo para modificar.**\n")
            nueva_ficha = await ctx.send(embed=escribir_nueva_ficha(campos, datos, "Editar"))
            # Preguntamos que campo modificar
            pregunta = await ctx.send("Campo:")

            def check_campo(respuesta_campo):
                # Comprobamos si los mensajes estan en su canal y son del mismo autor
                if respuesta_campo.author.id != ctx.author.id or respuesta_campo.channel.id != ctx.channel.id:
                    check = False
                else:
                    # Comprobamos que es del 1 al 8
                    try:
                        check = int(respuesta_campo.content) > 0 and int(respuesta_campo.content) <= 8
                    except:
                        check = False
                return (check)

            respuesta_campo = await bot.wait_for("message", check=(check_campo), timeout=60)
            await pregunta.delete()
            await respuesta_campo.delete()
            # Comprobamos el campo a modificar en concreto
            pregunta = await ctx.send("Modificar:")

            def check_campo_modificar(respuesta):
                # Comprobamos si los mensajes estan en su canal y son del mismo autor
                if respuesta.author.id != ctx.author.id or respuesta.channel.id != ctx.channel.id:
                    check = False
                else:
                    if int(respuesta_campo.content) in (1, 2, 3, 6):
                        check = len(respuesta.content) >= 1
                    if int(respuesta_campo.content) == 4:
                        check = len(respuesta.content) == 10
                    if int(respuesta_campo.content) == 5:
                        try:
                            check = int(respuesta.content) > 0 and int(respuesta.content) <= 14
                        except:
                            check = False
                    if int(respuesta_campo.content) == 7:
                        check = len(respuesta.content) == 17
                    if int(respuesta_campo.content) == 8:
                        check = respuesta.content == "Si" or respuesta.content == "No"
                return (check)

            respuesta = await bot.wait_for("message", check=(check_campo_modificar), timeout=60)
            await pregunta.delete()
            await respuesta.delete()
            # Identidicamos el campo SQL a modificar y modificamos los valores para la SQL
            if int(respuesta_campo.content) == 1:
                campo = "Nick"
                respuesta.content = '"' + respuesta.content + '"'
            elif int(respuesta_campo.content) == 2:
                campo = "Nombre"
                respuesta.content = '"' + respuesta.content + '"'
            elif int(respuesta_campo.content) == 3:
                campo = "Localidad"
                respuesta.content = '"' + respuesta.content + '"'
            elif int(respuesta_campo.content) == 4:
                campo = "FechaNacimiento"
                respuesta.content = datetime.datetime.strptime(str(respuesta.content) + " 00:00:00",
                                                               '%d-%m-%Y %H:%M:%S')
                respuesta.content = '"' + str(respuesta.content) + '"'
            elif int(respuesta_campo.content) == 5:
                campo = "Clase"
            elif int(respuesta_campo.content) == 6:
                campo = "DisponibilidadHoraria"
                respuesta.content = '"' + respuesta.content + '"'
            elif int(respuesta_campo.content) == 7:
                campo = "SteamID64"
                SteamHLL = update_steam_hll(respuesta.content, "SteamHLL")
                try:
                    if int(SteamHLL) < int(get_SteamHLL_Inicio):
                        get_SteamHLL_Inicio = SteamHLL
                        update_steam_hll(get_Steam, "SteamHLL_Inicio")
                except:
                    update_steam_hll(get_Steam, "SteamHLL")
            elif int(respuesta_campo.content) == 8:
                campo = "Streaming"
                respuesta.content = '"' + respuesta.content + '"'
            query = 'UPDATE Miembros set ' + str(campo) + ' = ' + str(respuesta.content) + ' WHERE idMiembro=' + str(
                member.id)
            sql_update(query)
            await ctx.channel.send("Campo modificado correctamente.")
        except asyncio.TimeoutError:
            await ctx.send("Tiempo de espera agotado (60s).")
        except Error:
            await ctx.channel.send(Error)


#################
# En desarrollo #
#################

def consultar_miembro_por_nick(nick):
    query = 'SELECT idMiembro,Nick,Nombre,Localidad,FechaNacimiento,Clase,DisponibilidadHoraria,SteamID64,SteamHLL_Inicio,Streaming from Miembros where Nick="' + str(
        nick) + '"'
    datos_miembro = sql_fetch(query)
    check = datos_miembro != []
    return check, datos_miembro


def is501(name):
    if name[0][0:8] == '501.es |':
        return True
    else:
        return False


def introducir_stats_partida(player_list, id_partida):
    for player in player_list:
        if is501(player):
            query = 'INSERT INTO Estadisticas (idMiembro,idPartida, Muertes, Bajas) VALUES(' + str(
                numpy.array(consultar_miembro_por_nick(player[0][9:40])[1][0][0])) + ', ' + str(
                id_partida[0]) + ',"' + str(player[2]) + '","' + str(
                player[1]) + '")'
            print(id_partida)
            print(query)
            sql_update(query)


def consultar_lista_miembros(player_list):
    error_list = ""
    error_check = False
    for player in player_list:
        if is501(player):
            miembro = consultar_miembro_por_nick(player[0][9:40])
            if not miembro[0]:
                error_check = True
                error_list += "No existe miembro " + player[0][9:40] + ".\n"
    return error_check, error_list


async def crear_partida(msg):
    campos = ['Contrincante', 'Aliados(1 = Si, 0 = No)', 'Resultado(1 = Ganado, 0 =  Perdido)', 'Puntuacion',
              'Jugadores', 'Tipo partida', 'Bando', 'Confirmar o repetir']
    datos = ['BestKorea', '1', '1', '5-0', '50',
             'Oficial', 'Aleman', '-']
    await msg.send("**Responde a las preguntas.**\nSi quieres volver a escribir un campo pon ``repetir``\n\n")
    nueva_ficha = await msg.send(embed=escribir_nueva_ficha(campos, datos, "Crear"))
    global index_c
    index_c = 0
    while index_c < len(campos):
        pregunta = await msg.send(str(campos[index_c]) + ":")

        # Comprobamos que los mensajes tienen los valores requeridos
        def check_ficha(respuesta):
            # Comprobamos si los mensajes estan en su canal y son del mismo autor
            if respuesta.author.id != msg.author.id or respuesta.channel.id != msg.channel.id:
                check = False
            else:
                check = False
                if str(campos[index_c]) in ("Contrincante", 'Tipo partida', 'Bando'):
                    check = len(respuesta.content) >= 1
                if str(campos[index_c]) == "Puntuacion":
                    check = len(respuesta.content) == 3
                if str(campos[index_c]) in ("Aliados(1 = Si, 0 = No)", "Resultado(1 = Ganado, 0 =  Perdido)"):
                    check = len(respuesta.content) == 1
                if str(campos[index_c]) == "Jugadores":
                    check = 0 < len(respuesta.content) < 3
                if str(campos[index_c]) == "Confirmar o repetir":
                    check = respuesta.content == "confirmar" or respuesta.content == "Confirmar"
                if respuesta.content == "repetir":
                    check = True
            return check

        respuesta = await bot.wait_for("message", check=check_ficha, timeout=60)
        # Borramos mensajes de preguntas y respuestas
        await pregunta.delete()
        await respuesta.delete()
        # Ordenamos los mensajes, si tiene repetir volvemos atras
        if respuesta.content == "repetir":
            if index_c == 0:
                index_c = 0
            else:
                index_c = index_c - 1
        else:
            datos[index_c] = respuesta.content
            await nueva_ficha.edit(embed=escribir_nueva_ficha(campos, datos, "Nueva"))
            index_c += 1
    # Introducimos los datos en la BD
    query = 'INSERT INTO Partidas (Contrincante, Aliados, Resultado, Puntuacion, Fecha, NJugadores, TipoPartida, ' \
            + 'Bando) VALUES("' + str(datos[0]) + '",' + str(datos[1]) + ', ' + str(datos[2]) + ', "' + str(datos[3]) \
            + '","' + str(datetime.date.today()) + '","' + str(datos[4]) + '","' + str(datos[5]) + '","' \
            + str(datos[6]) + '")'
    sql_update(query)
    return True, datos[0]


def check_last_insert(table, column_id):
    try:
        con = sql_connection()
        cursor = con.cursor()
        last_index = cursor.execute('SELECT max(' + str(column_id) + ') FROM ' + str(table))
        max_id = last_index.fetchone()[0]
        cursor.close()
        return max_id
    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
    finally:
        if con:
            con.close()


def consultar_partida(id_partida):
    try:
        query = ''
        if id_partida == 'all':
            res = sql_fetch('SELECT * From Partidas')
        else:
            res = sql_fetch('SELECT * From Partidas WHERE idPartida=' + str(id_partida))
        for row in res:
            query += ("ID = " + str(row[0]) + ", " + "contrincante = " + str(row[1]) + ", " + "aliados = "
                      + str(row[2]) + ", " + "resultado = " + str(row[3]) + ", " + "puntuacion = " + str(row[4])
                      + ", " + "fecha = " + str(row[5]) + ", " + "número de jugadores = " + str(row[6]) + ", "
                      + "Tipo de partida = " + str(row[7]) + ", " + "Bando = " + str(row[8]) + "\n")
        return query
    except:
        return ''


def borrar_partida(id_partida):
    try:
        con = sql_connection()
        cursor = con.cursor()
        cursor.execute('DELETE FROM Partidas WHERE idPartida=' + str(id_partida))
        cursor.close()
        return True, ""
    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
        return False, ""
    finally:
        if con:
            con.close()


def insertar_datos(msg, id_partida):
    for attachment_url in msg.message.attachments:
        with requests.Session() as session:
            player_list = list(
                csv.reader(session.get(str(attachment_url)).content.decode('utf-8').splitlines(), delimiter=','))
            result = consultar_lista_miembros(player_list)
            error_list = result[1]
            error_check = result[0]
            if not error_check:
                introducir_stats_partida(player_list, id_partida)
                return True, ''
            else:
                return False, str(error_list)


@bot.command(pass_context=True)
async def partida(msg, accion, *id_partida):
    if accion == 'crear':
        res = await crear_partida(msg)
        if res[0]:
            return await msg.send("Partida contra " + str(res[1]) + " creada con ID "
                                  + str(check_last_insert('Partidas', 'idPartida')))
        else:
            return await msg.send("No se ha podido crear la partida")
    elif accion == 'borrar':
        res = consultar_partida(id_partida[0])
        print(res)
        if len(res) > 1:
            delete = borrar_partida(id_partida[0])
            if delete[0]:
                return await msg.send("Partida con ID " + id_partida[0] + " borrada")
            else:
                return await msg.send("No se ha podido borrar la partida")
        else:
            return await msg.send('No se han encontrado resultados')
    elif accion == 'csv':
        res = insertar_datos(msg, id_partida)
        if res[0]:
            return await msg.send(str("Datos introducidos correctamente"))
        else:
            return await msg.send(res[1])
    elif accion == 'consultar':
        res = consultar_partida(id_partida[0])
        if len(res) > 1:
            return await msg.send(str(res))
        else:
            return await msg.send('No se han encontrado resultados')


bot.run(config['token'], reconnect=True)
