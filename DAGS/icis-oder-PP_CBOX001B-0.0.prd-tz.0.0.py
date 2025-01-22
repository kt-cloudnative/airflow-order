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
                , WORKFLOW_NAME='PP_CBOX001B',WORKFLOW_ID='0777d881017e4b35ae8094027c64cf98', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOX001B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0777d881017e4b35ae8094027c64cf98')

    cbox001bJob_vol = []
    cbox001bJob_volMnt = []
    cbox001bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbox001bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbox001bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbox001bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5886010b2b554e388012cb47b6b06cd7',
        'volumes': cbox001bJob_vol,
        'volume_mounts': cbox001bJob_volMnt,
        'env_from':cbox001bJob_env,
        'task_id':'cbox001bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.87',
        'arguments':["--job.name=cbox001bJob", "requestDate="+str(datetime.now()), "procDate=20241126"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0777d881017e4b35ae8094027c64cf98')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbox001bJob,
        Complete
    ]) 

    # authCheck >> cbox001bJob >> Complete
    workflow








