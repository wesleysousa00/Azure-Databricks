# v3 Código mais atualizado, com retry e failed notebook pai, e retry apenas dos notebooks que falharam.
import json
import time
from threading import Thread, Event
from queue import Queue

notebook_list_error = []
threads = []

error_event = Event()
q = Queue()
f = Queue()
work_count = 12
work_count_error = 4

def run_notebook(notebook, error_event, q):
    print(f"RUNNING: {notebook}")
    try:
        dbutils.notebook.run(notebook, 0)
        print(f"SUCCESS: {notebook}")
        if notebook in notebook_list_error:
            notebook_list_error.remove(notebook)

    except Exception as e:
        print(f"ERROR: {notebook}")
        error_event.set()  # Sinaliza que ocorreu um erro

        if notebook in notebook_list:
            notebook_list.remove(notebook) # remove da lista dos notebooks que rodaram

        if notebook not in notebook_list_error:
            notebook_list_error.append(notebook) # adiciona na lista de erros


def queue_notebooks(fila, lista):
    for notebook in lista:
        fila.put(notebook)


def run_tasks(function, q, error_event):
    while not q.empty():
        value = q.get()
        run_notebook(value, error_event, q)
        q.task_done()


def start_workers_threads(workers, fila):
    for i in range(workers):
        t = Thread(target=run_tasks, args=(run_notebook, fila, error_event))
        t.daemon = True
        t.start()
        threads.append(t)
    fila.join()


def retry_failed_notebooks(notebook_list_error, f):
    if len(notebook_list_error) == 0:
        print("\nSUCCESS", "Todos os notebooks filhos foram executados com sucesso.")

    else:
        count_error = 1
        while count_error < 3 and len(notebook_list_error) > 0:
            for error_item in notebook_list_error:
                print(f"RETRY: {count_error} {error_item} in 30seg ...")
            time.sleep(30)

            queue_notebooks(f, notebook_list_error)
            start_workers_threads(4, f)
            count_error += 1

        if len(notebook_list_error) > 0 and count_error == 3:
            print("ERROR", "Pelo menos um notebook filho falhou. O código pai também irá falhar.")
            raise Exception("Um ou mais notebooks filhos falharam.")
        else:
            print("\nSUCCESS", "Todos os notebooks filhos foram executados com sucesso.")


def run_notebooks_layers(notebook_list, f):

    if len(notebook_list) == 0:
        print("\nSUCCESS", "Todos os notebooks filhos foram executados com sucesso.")

    else:
        queue_notebooks(f, notebook_list)
        start_workers_threads(6, f)

        if len(notebook_list_error) > 0:
            print("ERROR", "Pelo menos um notebook filho falhou. O código pai também irá falhar.")
            raise Exception("Um ou mais notebooks filhos falharam.")
        else:
            print("\nSUCCESS", "Todos os notebooks filhos foram executados com sucesso.")
