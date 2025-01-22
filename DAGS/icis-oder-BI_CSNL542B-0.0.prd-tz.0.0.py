from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='BI_CSNL542B',WORKFLOW_ID='e8ea4c2f844e4c7bbad7fd322066a637', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSNL542B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 22, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e8ea4c2f844e4c7bbad7fd322066a637')

    csnl542bJob_vol = []
    csnl542bJob_volMnt = []
    csnl542bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnl542bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csnl542bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl542bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e9546742a9dc4e52a9460e015f7e8745',
        'volumes': csnl542bJob_vol,
        'volume_mounts': csnl542bJob_volMnt,
        'env_from':csnl542bJob_env,
        'task_id':'csnl542bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csnl542bJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e8ea4c2f844e4c7bbad7fd322066a637')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl542bJob,
        Complete
    ]) 

    # authCheck >> csnl542bJob >> Complete
    workflow








