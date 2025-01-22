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
                , WORKFLOW_NAME='bi-mprdhierPsacdSacdRedisJob-batch',WORKFLOW_ID='b6f3d1b6a58542c4b2ee87255f7f1705', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-mprdhierPsacdSacdRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b6f3d1b6a58542c4b2ee87255f7f1705')

    redisMprdhierPsacdSacdJob_vol = []
    redisMprdhierPsacdSacdJob_volMnt = []
    redisMprdhierPsacdSacdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisMprdhierPsacdSacdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisMprdhierPsacdSacdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisMprdhierPsacdSacdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisMprdhierPsacdSacdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisMprdhierPsacdSacdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a5ea06089f4b48e191492da7364bc3f5',
        'volumes': redisMprdhierPsacdSacdJob_vol,
        'volume_mounts': redisMprdhierPsacdSacdJob_volMnt,
        'env_from':redisMprdhierPsacdSacdJob_env,
        'task_id':'redisMprdhierPsacdSacdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisMprdhierPsacdSacdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b6f3d1b6a58542c4b2ee87255f7f1705')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisMprdhierPsacdSacdJob,
        Complete
    ]) 

    # authCheck >> redisMprdhierPsacdSacdJob >> Complete
    workflow








