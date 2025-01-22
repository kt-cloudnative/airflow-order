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
                , WORKFLOW_NAME='bi-mprdhierPsacdRedisJob-batch',WORKFLOW_ID='4adcdd5995004c918bd0c9430d7d4196', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-mprdhierPsacdRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4adcdd5995004c918bd0c9430d7d4196')

    redisMprdhierPsacdJob_vol = []
    redisMprdhierPsacdJob_volMnt = []
    redisMprdhierPsacdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisMprdhierPsacdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisMprdhierPsacdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisMprdhierPsacdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisMprdhierPsacdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisMprdhierPsacdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '46ba4783f8b2400ca09ace769d380072',
        'volumes': redisMprdhierPsacdJob_vol,
        'volume_mounts': redisMprdhierPsacdJob_volMnt,
        'env_from':redisMprdhierPsacdJob_env,
        'task_id':'redisMprdhierPsacdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisMprdhierPsacdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4adcdd5995004c918bd0c9430d7d4196')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisMprdhierPsacdJob,
        Complete
    ]) 

    # authCheck >> redisMprdhierPsacdJob >> Complete
    workflow








