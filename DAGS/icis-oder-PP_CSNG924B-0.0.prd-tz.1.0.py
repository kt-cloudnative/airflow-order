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
                , WORKFLOW_NAME='PP_CSNG924B',WORKFLOW_ID='70b9c9ee7fcb44d98a2f213d7548f637', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG924B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('70b9c9ee7fcb44d98a2f213d7548f637')

    csng924bJob_vol = []
    csng924bJob_volMnt = []
    csng924bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng924bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng924bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng924bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng924bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng924bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '19a40977f19c49c7a750f7e932214fee',
        'volumes': csng924bJob_vol,
        'volume_mounts': csng924bJob_volMnt,
        'env_from':csng924bJob_env,
        'task_id':'csng924bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng924bJob", "tranDate=20251129", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('70b9c9ee7fcb44d98a2f213d7548f637')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng924bJob,
        Complete
    ]) 

    # authCheck >> csng924bJob >> Complete
    workflow








