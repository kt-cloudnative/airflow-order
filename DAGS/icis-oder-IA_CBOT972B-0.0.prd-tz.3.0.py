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
                , WORKFLOW_NAME='IA_CBOT972B',WORKFLOW_ID='84058997a7e542989b85d3182f8abefb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT972B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('84058997a7e542989b85d3182f8abefb')

    cbot972bJob_vol = []
    cbot972bJob_volMnt = []
    cbot972bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot972bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot972bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot972bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot972bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot972bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '20caa5a842a146888b65c2b816ab1d77',
        'volumes': cbot972bJob_vol,
        'volume_mounts': cbot972bJob_volMnt,
        'env_from':cbot972bJob_env,
        'task_id':'cbot972bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.36',
        'arguments':["--job.name=cbot972bJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('84058997a7e542989b85d3182f8abefb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot972bJob,
        Complete
    ]) 

    # authCheck >> cbot972bJob >> Complete
    workflow








