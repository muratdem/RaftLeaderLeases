#include <string.h>

#include <Python.h>

#define MAX_FAILED_WRITES 50

typedef enum {
    READ,
    LIST_APPEND
} OpType;

typedef struct {
    OpType op_type;
    int execution_ts;
    int* value;
    int value_size;
    int success;
} LogEntry;

// 1 means success, 0 failure
static int log_entry_py2c(PyObject* py_entry, LogEntry* entry) {
    int retval = 0;
    PyObject* py_execution_ts = NULL;
    PyObject* py_value = NULL;
    PyObject* py_success = NULL;

    // Get op_type
    PyObject* py_op_type = PyObject_GetAttrString(py_entry, "op_type");
    PyObject* py_op_type_name = PyObject_GetAttrString(py_op_type, "name");
    const char* op_type_name = PyUnicode_AsUTF8(py_op_type_name);
    if (strcmp(op_type_name, "Read") == 0) {
        entry->op_type = READ;
    } else if (strcmp(op_type_name, "ListAppend") == 0) {
        entry->op_type = LIST_APPEND;
    } else {
        PyErr_SetString(PyExc_RuntimeError, "unrecognized op_type");
        goto FINISH;
    }

    // Get execution_ts
    py_execution_ts = PyObject_GetAttrString(py_entry, "execution_ts");
    entry->execution_ts = PyLong_AsLong(py_execution_ts);

    // Get value
    py_value = PyObject_GetAttrString(py_entry, "value");
    if (py_value == Py_None) {
        entry->value_size = 0;
        entry->value = NULL;
    } else if (PyLong_Check(py_value)) {
        entry->value_size = 1;
        entry->value = malloc(sizeof(int));
        entry->value[0] = PyLong_AsLong(py_value);
    } else if (PyList_Check(py_value)) {
        entry->value_size = (int)PyList_Size(py_value);
        entry->value = malloc(entry->value_size * sizeof(int));
        for (int i = 0; i < entry->value_size; ++i) {
            if (!PyLong_Check(PyList_GET_ITEM(py_value, i))) {
                PyErr_SetString(PyExc_RuntimeError, "value isn't an integer");
                goto FINISH;
            }
            entry->value[i] = PyLong_AsLong(PyList_GET_ITEM(py_value, i));
        }
    } else {
        PyErr_SetString(PyExc_RuntimeError, "value isn't an integer");
        goto FINISH;
    }

    // Does this entry represent a successful operation?
    py_success = PyObject_GetAttrString(py_entry, "success");
    entry->success = PyObject_IsTrue(py_success);

    // We successfully converted this entry.
    retval = 1;

    FINISH:
    Py_XDECREF(py_op_type_name);
    Py_XDECREF(py_op_type);
    Py_XDECREF(py_execution_ts);
    Py_XDECREF(py_success);
    Py_XDECREF(py_value);

    return retval;
}

// 1 means success, 0 failure
int linearize(LogEntry *log, int log_size, int *value, int value_size, LogEntry *failed_writes, int failed_writes_size) {
    int retval = 0;
    LogEntry *log_prime = NULL;
    LogEntry *new_failed_writes = NULL;
    if (log_size == 0) {
        return 1; // Empty history is already linearized.
    }

    // If there are simultaneous events, try ordering any linearization of them.
    int first_ts = log[0].execution_ts;

    for (int i = 0; i < log_size; i++) {
        // Try linearizing "entry" at history's start. No other entry's end can precede this entry's start.
        LogEntry entry = log[i];
        if (entry.execution_ts != first_ts) {
            // We've tried all the entries with minimal exec times, linearization failed.
            break;
        }

        // Create log_prime without the current entry.
        free(log_prime);
        log_prime = calloc(log_size - 1, sizeof(LogEntry));
        int log_prime_size = 0;
        for (int j = 0; j < log_size; j++) {
            if (i != j) {
                log_prime[log_prime_size++] = log[j];
            }
        }

        // Try letting the failed write take effect later or not at all.
        if (entry.op_type == LIST_APPEND && !entry.success) {
            if (failed_writes_size + 1 > MAX_FAILED_WRITES) {
                PyErr_SetString(PyExc_RuntimeError, "too many failed writes");
                goto FINISH;
            } else {
                LogEntry *new_failed_writes = calloc(failed_writes_size + 1, sizeof(LogEntry));
                memcpy(new_failed_writes, failed_writes, failed_writes_size * sizeof(LogEntry));
                new_failed_writes[failed_writes_size] = entry;
                int result = linearize(log_prime, log_prime_size, value, value_size, new_failed_writes, failed_writes_size + 1);
                free(new_failed_writes);
                if (result) {
                    retval = 1;
                    goto FINISH;
                }
            }
        }

        // The write succeeded.
        if (entry.op_type == LIST_APPEND && entry.success) {
            if (entry.value_size != 1) {
                PyErr_SetString(PyExc_RuntimeError, "LIST_APPEND with too many values");
                goto FINISH;
            }
            int new_value[value_size + 1];
            memcpy(new_value, value, value_size * sizeof(int));
            new_value[value_size] = entry.value[0];
            if (linearize(log_prime, log_prime_size, new_value, value_size + 1, failed_writes, failed_writes_size)) {
                retval = 1;
                goto FINISH;
            }
        }

        // Did this read get the value we expect given the entries we've linearized so far?
        if (entry.op_type == READ) {
            if (value_size != entry.value_size) {
                continue;
            }
            if (0 != memcmp(value, entry.value, value_size * sizeof(int))) {
                continue;
            }

            // The read got a value matching our expectation, try linearizing the rest of the log.
            if (linearize(log_prime, log_prime_size, value, value_size, failed_writes, failed_writes_size)) {
                retval = 1;
                goto FINISH;
            }
        }
    }

    // Linearization is failing so far, maybe one of the failed writes took effect?
    for (int i = 0; i < failed_writes_size; i++) {
        LogEntry f = failed_writes[i];
        free(new_failed_writes);
        new_failed_writes = calloc(failed_writes_size - 1, sizeof(LogEntry));
        int new_failed_writes_size = 0;
        for (int j = 0; j < failed_writes_size; j++) {
            if (i != j) {
                new_failed_writes[new_failed_writes_size++] = failed_writes[j];
            }
        }
        if (f.value_size != 1) {
            PyErr_SetString(PyExc_RuntimeError, "failed LIST_APPEND with too many values");
            goto FINISH;
        }
        int new_value[value_size + 1];
        memcpy(new_value, value, value_size * sizeof(int));
        new_value[value_size] = f.value[0];
        if (linearize(log, log_size, new_value, value_size + 1, new_failed_writes, new_failed_writes_size)) {
            retval = 1;
            goto FINISH;
        }
    }

    FINISH:
    free(new_failed_writes);
    free(log_prime);
    return retval;
}

static PyObject* do_linearizability_check(PyObject* self, PyObject* args) {
    PyObject *client_log;

    if (!PyArg_ParseTuple(args, "O", &client_log)) {
        return NULL;
    }

    if (!PyList_Check(client_log)) {
        PyErr_SetString(PyExc_TypeError, "First argument to linearize() must be a list");
        return NULL;
    }

    Py_ssize_t num_entries = PyList_Size(client_log);
    LogEntry* c_log = calloc(num_entries, sizeof(LogEntry));
    for (Py_ssize_t i = 0; i < num_entries; ++i) {
        PyObject *py_entry = PyList_GetItem(client_log, i);
        if (!log_entry_py2c(py_entry, &c_log[i])) {
            return NULL;
        }
    }

    LogEntry failed_writes[MAX_FAILED_WRITES];
    int value[0];
    if (!linearize(c_log, num_entries, value, 0, failed_writes, 0)) {
        PyErr_SetString(PyExc_RuntimeError, "not linearizable");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef linearizeMethods[] = {
    {"do_linearizability_check", do_linearizability_check, METH_VARARGS,
     "Check linearizability of sorted client log."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef linearizemodule = {
    PyModuleDef_HEAD_INIT,
    "linearize",
    NULL,
    -1,
    linearizeMethods
};

PyMODINIT_FUNC PyInit_linearize(void) {
    return PyModule_Create(&linearizemodule);
}
