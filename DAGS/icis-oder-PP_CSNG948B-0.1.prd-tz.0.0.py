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
                , WORKFLOW_NAME='PP_CSNG948B',WORKFLOW_ID='3a55dba6680d4ffb847258e15414b5cc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG948B-0.1.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 6, 10, 47, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3a55dba6680d4ffb847258e15414b5cc')

    csng948bJob_vol = []
    csng948bJob_volMnt = []
    csng948bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng948bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng948bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng948bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng948bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng948bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng948bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng948bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '915be35ca25241be85a5961786fcfd30',
        'volumes': csng948bJob_vol,
        'volume_mounts': csng948bJob_volMnt,
        'env_from':csng948bJob_env,
        'task_id':'csng948bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng948bJob", "applyDate=202411", "saIdSecEnum=1", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3a55dba6680d4ffb847258e15414b5cc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng948bJob,
        Complete
    ]) 

    # authCheck >> csng948bJob >> Complete
    workflow








