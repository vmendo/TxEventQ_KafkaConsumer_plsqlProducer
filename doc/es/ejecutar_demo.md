# Ejecutar la demo AIE TxEventQ OKafka

Runbook simple para ejecutar la demo completa.

Ejecutar los comandos desde la raiz del repositorio salvo que el paso indique un cambio de directorio.

## 1. Preparar la base de datos

Ejecutar los prerequisitos OKafka como `ADMIN`:

```bash
sql -S -L -name admin_adbdev2 @scripts/03_grant_okafka_client_prereqs.sql
```

Resetear y recrear el topic OKafka como `AIE`:

```bash
sql -S -L -name AIE @scripts/00_reset_demo.sql
sql -S -L -name AIE @scripts/01_create_okafka_topic.sql
```

## 2. Arrancar el consumidor

Abrir terminal 1 desde la raiz del repositorio:

```bash
./run_consumer.sh
```

En la primera ejecucion, el script pide lo que falte:

- ZIP del wallet ADB o directorio del wallet extraido, si no existe `wallet/tns_admin`.
- Password del usuario `AIE`, si no existe `.pwd.txt`.

La password se guarda localmente en `.pwd.txt` con permisos solo para el propietario. El wallet y `.pwd.txt` estan ignorados por Git.

Por defecto, el consumidor queda esperando records y no termina automaticamente. Dejalo arrancado mientras publicas mensajes y paralo con `Ctrl+C` cuando termine la demo.

Se pueden pasar argumentos opcionales al consumidor:

```bash
./run_consumer.sh --max-messages=10
./run_consumer.sh --idle-timeout-seconds=120
./run_consumer.sh --group-id=CGAIE$(date +%s)
```

## 3. Publicar mensajes

Abrir terminal 2 desde la raiz del repositorio:

```bash
sql -S -L -name AIE @scripts/02_publish_messages.sql
```

## 4. Resultado esperado

El consumidor debe imprimir 10 mensajes JSON de `AIE_EVENTS`. Como el modo por defecto es interactivo, paralo con `Ctrl+C` despues de revisar la salida.

Si lo ejecutas con `--max-messages=10`, termina con:

```text
Committed batch. Total consumed: 10
Consumer finished. Total consumed: 10
```

## 5. Notas

- Ejecutar el consumidor Java desde una shell con acceso de red al host de ADB.
- Si el consumidor muestra inicialmente `AIE_EVENTS--1`, dejalo ejecutando unos minutos. OKafka puede revocar el placeholder y asignar particiones reales despues.
- Usa `sql -S -L -name AIE @scripts/04_diagnose_okafka_topic.sql` para inspeccionar subscribers, asignaciones de particion y mensajes en cola.
- Un `group.id` nuevo con `auto.offset.reset=earliest` lee backlog retenido. Para una demo limpia de 10 mensajes, resetear/recrear el topic antes de arrancar el consumidor.
- El script de diagnostico es solo de lectura y no llama a browse/dequeue directo por AQ.
- El wallet queda preparado en `wallet/tns_admin`.
