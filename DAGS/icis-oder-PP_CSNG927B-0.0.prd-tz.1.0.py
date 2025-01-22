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
                , WORKFLOW_NAME='PP_CSNG927B',WORKFLOW_ID='20ffeb547d0443c59cdf14801c8447c4', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG927B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('20ffeb547d0443c59cdf14801c8447c4')

    csng927bJob_vol = []
    csng927bJob_volMnt = []
    csng927bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng927bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng927bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng927bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng927bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng927bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b064b7ae956b4be19c08d23949a6554b',
        'volumes': csng927bJob_vol,
        'volume_mounts': csng927bJob_volMnt,
        'env_from':csng927bJob_env,
        'task_id':'csng927bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng927bJob", "duplPrvnDate=${YYYYMMDDHHMISSSSS}", "tranDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('20ffeb547d0443c59cdf14801c8447c4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng927bJob,
        Complete
    ]) 

    # authCheck >> csng927bJob >> Complete
    workflow








