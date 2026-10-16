"""Запуск: python test_sandbox.py  (сервис должен работать на :8000)"""
import time
import requests

API = "http://localhost:8000"

TESTS = [
    ("hello python",   "python", 'print("hi")',                                   "ok"),
    ("hello c",        "c",      '#include <stdio.h>\nint main(){puts("hi");return 0;}', "ok"),
    ("hello go",       "go",     'package main\nimport "fmt"\nfunc main(){fmt.Println("hi")}', "ok"),
    ("hello java",     "java",   'public class Main{public static void main(String[] a){System.out.println("hi");}}', "ok"),
    ("hello rust",     "rust",   'fn main(){println!("hi");}',                    "ok"),
    ("hello node",     "javascript", 'console.log("hi")',                         "ok"),
    ("ошибка компиляции", "c",   "int main(){ broken }",                          "compilation_error"),
    ("runtime error",  "python", "1/0",                                           "runtime_error"),
    ("ЕСТ ПАМЯТЬ py",  "python", "d=[]\nwhile True: d.append(b'x'*10_000_000)",   "memory_limit_exceeded"),
    ("ЕСТ ПАМЯТЬ c",   "c",      "#include <stdlib.h>\n#include <string.h>\nint main(){while(1){char*p=malloc(10000000);memset(p,1,10000000);}}", "memory_limit_exceeded"),
    ("бесконечный цикл", "python", "while True: pass",                            "timeout"),
    ("fork-бомба",     "python", "import os\nwhile True: os.fork()",              None),   # главное - сервис жив
    ("сеть",           "python", "import socket\nsocket.create_connection(('8.8.8.8',53),2)", "runtime_error"),
    ("бесконечный print", "python", "while True: print('x'*1000)",                "timeout"),
]


def run(lang, code):
    job = requests.post(f"{API}/run", json={"language": lang, "code": code}).json()["id"]
    while True:
        r = requests.get(f"{API}/run/{job}").json()
        if r["state"] == "finished":
            return r
        time.sleep(0.3)


for name, lang, code, expected in TESTS:
    r = run(lang, code)
    ok = expected is None or r["status"] == expected
    print(f"{'PASS' if ok else 'FAIL'}  {name:22} -> {r['status']:22} exit={r['exit_code']} t={r['time']}")

assert requests.get(f"{API}/health").status_code == 200, "сервис упал!"
print("\nСервис жив. Проверь, что контейнеры не копятся:  docker ps -a")
