import qarnot

class QarnotFaceswapWrapper:
    connect = None

    def test_values(self, values):
        print(values)

    def create_connection(self, info):
        if self.connect is None:
            self.connect = qarnot.connection.Connection(client_token=info["token"], cluster_url=info["cluster_url"])

    def start_info(self, values):
        self.create_connection(values)
        self.add_init_files(values)

        return {"snap_period" : 180,
                "docker_repo": "guillaumenebieqarnot/docker-hub-guillaume",
                "docker_tag": "faceswap_docker_gpu.1.0",
                "docker_cmd":"",
                "result":None,
                "resources":[],
                "task_name":"",
                "profile":"docker-nvidia-bi-a6000-batch",
                "instances":1,
                "custer_url":"https://api.qarnot.com",
                "token": values["pwd"],
                "task_uuid":"",
                }

    def retrieve_bucket_files(self, name, values):
        info = self.start_info(values)
        return self.connect.retrieve_or_create_bucket(name).list_files()

    def retrieve_task(self, info):
        return self.connect.retrieve_task(info["task_uuid"])

    def launch_task(self, info):
        task = self.connect.create_task(info["task_name"], info["profile"], info["instances"])
        task.constants['DOCKER_REPO'] = info["docker_repo"]
        task.constants['DOCKER_TAG'] = info["docker_tag"]
        task.constants['DOCKER_CMD'] = info["docker_cmd"]
        task.snapshot(info["snap_period"])
        task.results = info["result"]
        task.resources = info["ressource"]
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
        info = self.start_info(values)
        info["docker_cmd"] = "extract.sh"
        info["task_name"] = values["task-extract"]
        resources_names = [values["bucket-init"]]
        info.resources = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info.result = self.connect.retrieve_or_create_bucket(values["bucket-extract"])
        return self.launch_task(info)

    def train(self, values):
        info = self.start_info(values)
        info["docker_cmd"] = "train.sh"
        info["task_name"] = values["task-train"]
        resources_names = [values["bucket-init"], values["bucket-extract"]]
        info.resources = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info.result = self.connect.retrieve_or_create_bucket(values["bucket-train"])
        return self.launch_task(info)

    def prepare_convertion(self, values):
        info = self.start_info(values)
        info["docker_cmd"] = "prepare_convert.sh"
        info["task_name"] = values["task-prepare-convert"]
        resources_names = [values["bucket-init"]]
        info.resources = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info.result = self.connect.retrieve_or_create_bucket(values["bucket-prepare-convert"])
        return self.launch_task(info)

    def convert(self, values):
        info = self.start_info(values)
        info["docker_cmd"] = "convert.sh"
        info["task_name"] = values["task-convert"]
        resources_names = [values["bucket-init"], values["bucket-train"], values["bucket-prepare-convert"]]
        info.resources = [ self.connect.retrieve_or_create_bucket(bucket_name) for bucket_name in resources_names ]
        info.result = self.connect.retrieve_or_create_bucket(values[""])
        return self.launch_task(info)
