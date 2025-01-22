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
                , WORKFLOW_NAME='bi-csyscdAndCsysordtypeRedisJob-batch',WORKFLOW_ID='a2ae4e524f3c456fafe9af0da073c4d7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-csyscdAndCsysordtypeRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a2ae4e524f3c456fafe9af0da073c4d7')

    redisCsyscdAndCsysordtypeJob_vol = []
    redisCsyscdAndCsysordtypeJob_volMnt = []
    redisCsyscdAndCsysordtypeJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisCsyscdAndCsysordtypeJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisCsyscdAndCsysordtypeJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisCsyscdAndCsysordtypeJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisCsyscdAndCsysordtypeJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisCsyscdAndCsysordtypeJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ea9d245c88924e078f089cb8a03500aa',
        'volumes': redisCsyscdAndCsysordtypeJob_vol,
        'volume_mounts': redisCsyscdAndCsysordtypeJob_volMnt,
        'env_from':redisCsyscdAndCsysordtypeJob_env,
        'task_id':'redisCsyscdAndCsysordtypeJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisCsyscdAndCsysordtypeJob", "date=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a2ae4e524f3c456fafe9af0da073c4d7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisCsyscdAndCsysordtypeJob,
        Complete
    ]) 

    # authCheck >> redisCsyscdAndCsysordtypeJob >> Complete
    workflow








