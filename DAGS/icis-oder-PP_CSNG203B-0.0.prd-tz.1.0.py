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
                , WORKFLOW_NAME='PP_CSNG203B',WORKFLOW_ID='d000ac46f2d142e39c6d25791591f2a2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG203B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 4, 10, 52, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d000ac46f2d142e39c6d25791591f2a2')

    csng203bJob_vol = []
    csng203bJob_volMnt = []
    csng203bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng203bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng203bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng203bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng203bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng203bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b28d8dabbcbe4e48929f5c74cb7b69c2',
        'volumes': csng203bJob_vol,
        'volume_mounts': csng203bJob_volMnt,
        'env_from':csng203bJob_env,
        'task_id':'csng203bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.51',
        'arguments':["--job.name=csng203bJob", "workMonth=${YYYYMM}", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d000ac46f2d142e39c6d25791591f2a2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng203bJob,
        Complete
    ]) 

    # authCheck >> csng203bJob >> Complete
    workflow








