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
                , WORKFLOW_NAME='bi-csyscdAndMprdcdRedisJob-batch',WORKFLOW_ID='c7a06b6979014fa392c58886d90f5973', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-csyscdAndMprdcdRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c7a06b6979014fa392c58886d90f5973')

    redisCsyscdAndMprdcdJob_vol = []
    redisCsyscdAndMprdcdJob_volMnt = []
    redisCsyscdAndMprdcdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisCsyscdAndMprdcdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisCsyscdAndMprdcdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisCsyscdAndMprdcdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisCsyscdAndMprdcdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisCsyscdAndMprdcdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f67bb2cda6744fb087ebe0127a702b98',
        'volumes': redisCsyscdAndMprdcdJob_vol,
        'volume_mounts': redisCsyscdAndMprdcdJob_volMnt,
        'env_from':redisCsyscdAndMprdcdJob_env,
        'task_id':'redisCsyscdAndMprdcdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisCsyscdAndMprdcdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c7a06b6979014fa392c58886d90f5973')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisCsyscdAndMprdcdJob,
        Complete
    ]) 

    # authCheck >> redisCsyscdAndMprdcdJob >> Complete
    workflow








