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
                , WORKFLOW_NAME='bi-csyscdAndCsysprodvscdRedisJob-batch',WORKFLOW_ID='3a8e12dc03c34b6b8e53f78602f6f606', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-csyscdAndCsysprodvscdRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3a8e12dc03c34b6b8e53f78602f6f606')

    redisCsyscdAndCsysprodvscdJob_vol = []
    redisCsyscdAndCsysprodvscdJob_volMnt = []
    redisCsyscdAndCsysprodvscdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisCsyscdAndCsysprodvscdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisCsyscdAndCsysprodvscdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisCsyscdAndCsysprodvscdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisCsyscdAndCsysprodvscdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisCsyscdAndCsysprodvscdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '571d3a0f20d24a0c909930a23a43db19',
        'volumes': redisCsyscdAndCsysprodvscdJob_vol,
        'volume_mounts': redisCsyscdAndCsysprodvscdJob_volMnt,
        'env_from':redisCsyscdAndCsysprodvscdJob_env,
        'task_id':'redisCsyscdAndCsysprodvscdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisCsyscdAndCsysprodvscdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3a8e12dc03c34b6b8e53f78602f6f606')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisCsyscdAndCsysprodvscdJob,
        Complete
    ]) 

    # authCheck >> redisCsyscdAndCsysprodvscdJob >> Complete
    workflow








