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
                , WORKFLOW_NAME='PP_CSNG622B',WORKFLOW_ID='7f76a10566454763b57f6aebdb580a11', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG622B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7f76a10566454763b57f6aebdb580a11')

    csng622bJob_vol = []
    csng622bJob_volMnt = []
    csng622bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng622bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng622bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng622bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng622bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng622bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '54a62ba0c5d44967be5d7a150387124b',
        'volumes': csng622bJob_vol,
        'volume_mounts': csng622bJob_volMnt,
        'env_from':csng622bJob_env,
        'task_id':'csng622bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng622bJob", "procMonth=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7f76a10566454763b57f6aebdb580a11')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng622bJob,
        Complete
    ]) 

    # authCheck >> csng622bJob >> Complete
    workflow








