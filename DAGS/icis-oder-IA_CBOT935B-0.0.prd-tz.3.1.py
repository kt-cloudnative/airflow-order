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
                , WORKFLOW_NAME='IA_CBOT935B',WORKFLOW_ID='1e7700af24e44ea8ad586c3cf756aadb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT935B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1e7700af24e44ea8ad586c3cf756aadb')

    cbot935bJob_vol = []
    cbot935bJob_volMnt = []
    cbot935bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot935bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot935bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot935bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot935bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot935bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e1343f9f4ad24abba6e505a85528abb3',
        'volumes': cbot935bJob_vol,
        'volume_mounts': cbot935bJob_volMnt,
        'env_from':cbot935bJob_env,
        'task_id':'cbot935bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.7.1.5',
        'arguments':["--job.name=cbot935bJob", "requestDate="+str(datetime.now()), "procDate=20241129", "argc=3", "argv=A"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1e7700af24e44ea8ad586c3cf756aadb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot935bJob,
        Complete
    ]) 

    # authCheck >> cbot935bJob >> Complete
    workflow








