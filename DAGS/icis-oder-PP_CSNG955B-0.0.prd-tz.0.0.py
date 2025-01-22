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
                , WORKFLOW_NAME='PP_CSNG955B',WORKFLOW_ID='37dc594ebcee4273baa71d6502ef88cd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG955B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 6, 27, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('37dc594ebcee4273baa71d6502ef88cd')

    csng955bJob_vol = []
    csng955bJob_volMnt = []
    csng955bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng955bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng955bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng955bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng955bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng955bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a0bb296f9e7f494cb905b9d3ae2e7ec5',
        'volumes': csng955bJob_vol,
        'volume_mounts': csng955bJob_volMnt,
        'env_from':csng955bJob_env,
        'task_id':'csng955bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng955bJob", "expPamDate=${YYYYMMDD}", "testRequestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('37dc594ebcee4273baa71d6502ef88cd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng955bJob,
        Complete
    ]) 

    # authCheck >> csng955bJob >> Complete
    workflow








