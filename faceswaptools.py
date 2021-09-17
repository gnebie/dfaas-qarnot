import qarnot

import json
from IPython.display import display, clear_output

class QarnotFaceswapWrapper:
    connect = None

    def test_values(self, values):
        print(json.dumps(values, sort_keys=False, indent=4))

    def print_dump(self, value):
        print(json.dumps(value, sort_keys=False, indent=4))


    def print_bucket_dump(self, value_name, values, output):
        bucket_files = self.retrieve_bucket_files(values[value_name], values)
        if bucket_files:
            with output:
                clear_output()
                print(json.dumps(bucket_files, sort_keys=False, indent=4))

    def print_task_dump(self, value_name, values, output):
        with output:
            clear_output()
            try:
                task = self.retrieve_task_by_name(values[value_name], values)
            except Exception as ex:
                print("Error found:")
                print(ex)
                return
            if task:
                print("Name :" + values[value_name])
                print("Status :" + task.state)
                print("Creation time :" + str(task.creation_date))
                print("Execution time :" + str(task.status.execution_time))
                #print("Execution time :" + str(task.execution_time))

    def restart_task(self, value_name, values, output):
        with output:
            clear_output()
            try:
                task = self.retrieve_task_by_name(values[value_name], values)
            except Exception as ex:
                print("Error found:")
                print(ex)
                return
            if task:
                task.delete()
                print("Task delete : " + values[value_name])
                if "extract" in value_name:
                    task = self.extract(values)
                elif "train" in value_name:
                    task = self.train(values)
                elif "prepare" in value_name:
                    task = self.prepare(values)
                elif "convert" in value_name:
                    task = self.convert(values)
                if task:
                    print("Task launched!")
                    print("Status : " + task.state)
                    print("Uuid: " + task.uuid)

    def remove_task(self, value_name, values, output):
        with output:
            clear_output()
            try:
                task = self.retrieve_task_by_name(values[value_name], values)
            except Exception as ex:
                print("Error found:")
                print(ex)
                return
            if task:
                task.delete()
                print("Task delete : " + values[value_name])

    """
    def tasks_page_filter_name(name):
        filters = qarnot._filter.Filters.equal("name", name)
        url = qarnot.get_url('paginate tasks')
        result = self.connect._page_call(url, self._paginate_request(filters, token, maximum))
        data = [Task.from_json(self, task, summary) for task in result["data"]]
        return qarnot.PaginateResponse(token=result.get("token", token), next_token=result["nextToken"], is_truncated=result["isTruncated"], page_data=data)
    """


    def create_connection(self):
        if self.connect is None:
            try:
                self.connect = qarnot.connection.Connection(client_token=self.info["token"], cluster_url=self.info["cluster_url"])
            except Exception as ex:
                raise ValueError("Missing or invalid token.")

    def start_info(self, values):
        profil = "docker-nvidia-bi-a6000-batch"
        if values["material"] == "No":
            profil = "docker-batch"
        self.info = {"snap_period" : 180,
                "docker_repo": "guillaumenebieqarnot/docker-hub-guillaume",
                "docker_tag": "faceswap_docker_gpu.1.0",
                "docker_cmd":"",
                "result":None,
                "resources":[],
                "task_name":"",
                "profile":profil,
                "instances":1,
                "cluster_url":"https://api.qarnot.com",
                "token": values["pwd"],
                }
        try:
            self.create_connection()
        except ValueError as ex:
            print(ex)
            return None
            # self.add_init_files(values)
        return self.info

    def retrieve_bucket_files(self, name, values):
        if not self.start_info(values):
            return None
        bucket = self.connect.retrieve_or_create_bucket(name)
        list_file = bucket.list_files()
        return list(set(map(lambda x : x.key, list_file)))

    def upload_bucket_folder(self, name, directory, values):
        if self.start_info(values):
            self.connect.retrieve_or_create_bucket(name).add_directory(directory)

    def clean_bucket_folder(self, name, directory, values):
        if self.start_info(values):
            bucket = self.connect.retrieve_or_create_bucket(name).delete()

    def retrieve_task_by_name(self, name, values):
        if self.start_info(values):
            return self.connect.retrieve_task(name)

    def launch_task(self, info):
        task = self.connect.create_task(info["task_name"], info["profile"], info["instances"], info["task_name"])
        task.constants['DOCKER_REPO'] = info["docker_repo"]
        task.constants['DOCKER_TAG'] = info["docker_tag"]
        task.constants['DOCKER_CMD'] = info["docker_cmd"]
        task.snapshot(info["snap_period"])
        task.results = info["result"]
        task.resources = info["resources"]
        task.submit()
        return task

    def add_init_files(self, values):
        bucket_init = self.connect.retrieve_or_create_bucket(values["bucket-init"])
        if values["train-folder-a"]:
            bucket_init.add_directory(values["train-folder-a"])
        if values["train-folder-b"]:
            bucket_init.add_directory(values["train-folder-b"])
        if values["convert-folder-a"]:
            bucket_init.add_directory(values["convert-folder-a"])

    def extract(self, values):
        task_name = values["extract-task"]
        info = self.start_info(values)
        if not info:
            return None
        try:
            self.retrieve_task_by_name(task_name, values)
            print("Task " + task_name + " already created")
            return None
        except Exception:
            pass
        info["docker_cmd"] = "extract.sh"
        info["task_name"] = task_name
        resources_names = [values["init-bucket"]]
        info["resources"] = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info["result"] = self.connect.retrieve_or_create_bucket(values["extract-bucket"])
        return self.launch_task(info)

    def train(self, values):
        task_name = values["train-task"]
        info = self.start_info(values)
        if not info:
            return None
        try:
            self.retrieve_task_by_name(task_name, values)
            print("Task " + task_name + " already created")
            return None
        except Exception:
            pass
        info["docker_cmd"] = "train.sh"
        info["task_name"] = task_name
        resources_names = [values["init-bucket"], values["extract-bucket"]]
        info["resources"] = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info["result"] = self.connect.retrieve_or_create_bucket(values["train-bucket"])
        return self.launch_task(info)

    def prepare_convertion(self, values):
        task_name = values["prepare-task-task"]
        info = self.start_info(values)
        if not info:
            return None
        try:
            self.retrieve_task_by_name(task_name, values)
            print("Task " + task_name + " already created")
            return None
        except Exception:
            pass
        info["docker_cmd"] = "prepare_convert.sh"
        info["task_name"] = task_name
        resources_names = [values["init-bucket"]]
        info["resources"] = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info["result"] = self.connect.retrieve_or_create_bucket(values["prepare-bucket"])
        return self.launch_task(info)

    def convert(self, values):
        task_name = values["convert-task"]
        info = self.start_info(values)
        if not info:
            return None
        try:
            self.retrieve_task_by_name(task_name, values)
            print("Task " + task_name + " already created")
            return None
        except Exception:
            pass
        info["docker_cmd"] = "convert.sh"
        info["task_name"] = task_name
        resources_names = [values["init-bucket"], values["train-bucket"], values["prepare-bucket"]]
        info["resources"] = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info["result"] = self.connect.retrieve_or_create_bucket(values[""])
        return self.launch_task(info)
